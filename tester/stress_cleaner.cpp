// stress_cleaner.cpp (Linux)
// build: g++ -O2 -std=c++20 -pthread stress_cleaner.cpp -o stress_cleaner

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>

#ifdef __GLIBC__
  #include <malloc.h> // malloc_trim
#endif

using namespace std::chrono;

static long page_size() {
    static long ps = ::sysconf(_SC_PAGESIZE);
    return ps > 0 ? ps : 4096;
}

static std::optional<uint64_t> read_kb_from_status(pid_t pid, const std::string& key) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/status");
    if (!in) return std::nullopt;
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind(key, 0) == 0) {
            std::istringstream ss(line);
            std::string k, unit;
            uint64_t val = 0;
            ss >> k >> val >> unit;
            return val; // kB
        }
    }
    return std::nullopt;
}

static std::optional<uint64_t> read_proc_ticks(pid_t pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
    if (!in) return std::nullopt;

    std::string line;
    std::getline(in, line);
    if (line.empty()) return std::nullopt;

    // pid (comm) state ...
    auto rparen = line.rfind(')');
    if (rparen == std::string::npos) return std::nullopt;

    std::string rest = (rparen + 2 < line.size()) ? line.substr(rparen + 2) : "";
    std::istringstream ss(rest);
    std::vector<std::string> tok;
    tok.reserve(64);
    std::string t;
    while (ss >> t) tok.push_back(t);

    // In rest tokens: [0]=state, so utime is field14 => rest[11], stime field15 => rest[12]
    if (tok.size() <= 12) return std::nullopt;

    try {
        uint64_t ut = std::stoull(tok[11]);
        uint64_t st = std::stoull(tok[12]);
        return ut + st;
    } catch (...) {
        return std::nullopt;
    }
}

static void touch_pages(uint8_t* p, size_t bytes) {
    const size_t ps = (size_t)page_size();
    for (size_t i = 0; i < bytes; i += ps) p[i] = (uint8_t)(i ^ (i >> 8));
    if (bytes) p[bytes - 1] = 1;
}

static void cpu_burn_worker(std::atomic<bool>& stop, std::atomic<uint64_t>& sink) {
    uint64_t x = (uint64_t)reinterpret_cast<uintptr_t>(&stop);
    while (!stop.load(std::memory_order_relaxed)) {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        x ^= (x >> 33);
        x *= 0xff51afd7ed558ccdULL;
        x ^= (x >> 33);
        sink.fetch_add(x, std::memory_order_relaxed);
    }
}

struct MonCfg {
    pid_t pid;
    int interval_ms;
    std::atomic<bool>* stop;
};

static void monitor_loop(MonCfg cfg) {
    const double hz = (double)::sysconf(_SC_CLK_TCK);

    auto t0 = steady_clock::now();
    auto prev_t = t0;
    uint64_t prev_ticks = read_proc_ticks(cfg.pid).value_or(0);

    while (!cfg.stop->load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.interval_ms));

        auto now = steady_clock::now();
        double dt = duration<double>(now - prev_t).count();
        double ts = duration<double>(now - t0).count();

        auto ticks_opt = read_proc_ticks(cfg.pid);
        auto rss_kb = read_kb_from_status(cfg.pid, "VmRSS:");
        auto vms_kb = read_kb_from_status(cfg.pid, "VmSize:");

        if (!ticks_opt || !rss_kb || !vms_kb) {
            std::cout << std::fixed << std::setprecision(2)
                      << "[t=" << ts << "s] pid=" << cfg.pid << " <no /proc>\n";
            std::cout.flush();
            prev_t = now;
            continue;
        }

        uint64_t ticks = *ticks_opt;
        uint64_t dticks = (ticks >= prev_ticks) ? (ticks - prev_ticks) : 0;

        double cpu_pct = 0.0;
        if (dt > 0.0 && hz > 0.0) cpu_pct = 100.0 * ((double)dticks / (hz * dt)); // multi-thread => >100

        std::cout << std::fixed << std::setprecision(2)
                  << "[t=" << ts << "s] pid=" << cfg.pid
                  << " rss=" << (*rss_kb / 1024.0) << "MB"
                  << " vms=" << (*vms_kb / 1024.0) << "MB"
                  << " cpu=" << cpu_pct << "%\n";
        std::cout.flush();

        prev_t = now;
        prev_ticks = ticks;
    }
}

enum class MemMode { none, malloc_chunks, mmap, mmap_keep };

struct MemState {
    MemMode mode = MemMode::none;
    size_t total_bytes = 0;

    std::vector<void*> chunks;
    size_t chunk_bytes = 0;

    void* mmap_ptr = nullptr;
};

static MemMode parse_mem_mode(const std::string& s) {
    if (s == "none") return MemMode::none;
    if (s == "malloc_chunks") return MemMode::malloc_chunks;
    if (s == "mmap") return MemMode::mmap;
    if (s == "mmap_keep") return MemMode::mmap_keep;
    throw std::runtime_error("bad --mem-mode (none|malloc_chunks|mmap|mmap_keep)");
}

static void alloc_memory(MemState& st, int mem_mb, MemMode mode, int chunk_mb, bool touch) {
    st.mode = mode;
    st.total_bytes = (size_t)mem_mb * 1024ull * 1024ull;
    if (st.total_bytes == 0 || mode == MemMode::none) return;

    if (mode == MemMode::malloc_chunks) {
        st.chunk_bytes = (size_t)chunk_mb * 1024ull * 1024ull;
        if (st.chunk_bytes == 0) st.chunk_bytes = 4ull * 1024ull * 1024ull;

        size_t n = (st.total_bytes + st.chunk_bytes - 1) / st.chunk_bytes;
        st.chunks.reserve(n);

        for (size_t i = 0; i < n; ++i) {
            size_t cb = st.chunk_bytes;
            if (i + 1 == n) {
                size_t used = i * st.chunk_bytes;
                cb = (used < st.total_bytes) ? (st.total_bytes - used) : 0;
            }
            void* p = std::malloc(cb);
            if (!p) throw std::runtime_error("malloc chunk failed");
            st.chunks.push_back(p);
            if (touch && cb) touch_pages((uint8_t*)p, cb);
        }
        return;
    }

    if (mode == MemMode::mmap || mode == MemMode::mmap_keep) {
        void* p = ::mmap(nullptr, st.total_bytes, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
        st.mmap_ptr = p;
        if (touch) touch_pages((uint8_t*)p, st.total_bytes);
        return;
    }

    throw std::runtime_error("unknown mem mode");
}

static void free_memory(MemState& st, bool do_trim) {
    if (st.mode == MemMode::malloc_chunks) {
        for (void* p : st.chunks) std::free(p);
        st.chunks.clear();
        st.chunks.shrink_to_fit();
#ifdef __GLIBC__
        if (do_trim) ::malloc_trim(0);
#else
        (void)do_trim;
#endif
    } else if (st.mode == MemMode::mmap) {
        if (st.mmap_ptr) ::munmap(st.mmap_ptr, st.total_bytes);
        st.mmap_ptr = nullptr;
    } else if (st.mode == MemMode::mmap_keep) {
        if (st.mmap_ptr && st.total_bytes) ::madvise(st.mmap_ptr, st.total_bytes, MADV_DONTNEED);
    }
}

struct Opt {
    std::string mode = "threads"; // threads|fork
    int threads = 16;
    int cpu_s = 15;
    int pause_s = 25;
    int iterations = 1;

    int mem_mb = 0;
    MemMode mem_mode = MemMode::malloc_chunks;
    int chunk_mb = 4;
    bool touch = true;
    bool trim = true;

    int monitor_ms = 500;
    int kill_after_s = 0; // fork mode
};

static std::string next_arg(int& i, int argc, char** argv) {
    if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + argv[i]);
    return argv[++i];
}

static Opt parse(int argc, char** argv) {
    Opt o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--mode") o.mode = next_arg(i, argc, argv);
        else if (a == "--threads") o.threads = std::stoi(next_arg(i, argc, argv));
        else if (a == "--cpu-s") o.cpu_s = std::stoi(next_arg(i, argc, argv));
        else if (a == "--pause-s") o.pause_s = std::stoi(next_arg(i, argc, argv));
        else if (a == "--iterations") o.iterations = std::stoi(next_arg(i, argc, argv));
        else if (a == "--mem-mb") o.mem_mb = std::stoi(next_arg(i, argc, argv));
        else if (a == "--mem-mode") o.mem_mode = parse_mem_mode(next_arg(i, argc, argv));
        else if (a == "--chunk-mb") o.chunk_mb = std::stoi(next_arg(i, argc, argv));
        else if (a == "--monitor-ms") o.monitor_ms = std::stoi(next_arg(i, argc, argv));
        else if (a == "--kill-after-s") o.kill_after_s = std::stoi(next_arg(i, argc, argv));
        else if (a == "--no-touch") o.touch = false;
        else if (a == "--no-trim") o.trim = false;
        else if (a == "--help" || a == "-h") {
            std::cout <<
R"(Usage:
  ./stress_cleaner --mode threads|fork --threads N --cpu-s SEC --pause-s SEC
                   --mem-mb MB --mem-mode none|malloc_chunks|mmap|mmap_keep
                   --chunk-mb MB --monitor-ms MS --iterations N
                   [--kill-after-s SEC] [--no-touch] [--no-trim]

Notes:
  - Run with ./stress_cleaner (current dir is not in PATH).
)";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown arg: " + a);
        }
    }
    if (o.threads <= 0) o.threads = 1;
    if (o.iterations <= 0) o.iterations = 1;
    if (o.monitor_ms < 50) o.monitor_ms = 50;
    return o;
}

static void run_workload_once(const Opt& o) {
    MemState mem;
    if (o.mem_mb > 0 && o.mem_mode != MemMode::none) {
        alloc_memory(mem, o.mem_mb, o.mem_mode, o.chunk_mb, o.touch);
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> sink{0};

    std::vector<std::thread> workers;
    workers.reserve((size_t)o.threads);
    for (int i = 0; i < o.threads; ++i) workers.emplace_back(cpu_burn_worker, std::ref(stop), std::ref(sink));

    std::this_thread::sleep_for(std::chrono::seconds(o.cpu_s));
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : workers) th.join();

    free_memory(mem, o.trim);
    std::this_thread::sleep_for(std::chrono::seconds(o.pause_s));
}

static int threads_mode(const Opt& o) {
    pid_t pid = ::getpid();
    std::atomic<bool> mon_stop{false};
    std::thread mon(monitor_loop, MonCfg{pid, o.monitor_ms, &mon_stop});

    for (int it = 1; it <= o.iterations; ++it) {
        std::cout << "=== iteration " << it << "/" << o.iterations << " (threads) ===\n";
        std::cout.flush();
        run_workload_once(o);
    }

    mon_stop.store(true, std::memory_order_relaxed);
    mon.join();
    return 0;
}

static int fork_mode(const Opt& o) {
    for (int it = 1; it <= o.iterations; ++it) {
        std::cout << "=== iteration " << it << "/" << o.iterations << " (fork) ===\n";
        std::cout.flush();

        pid_t child = ::fork();
        if (child < 0) { perror("fork"); return 1; }

        if (child == 0) {
            try {
                run_workload_once(o);
                _exit(0);
            } catch (const std::exception& e) {
                std::cerr << "child error: " << e.what() << "\n";
                _exit(2);
            }
        }

        std::atomic<bool> mon_stop{false};
        std::thread mon(monitor_loop, MonCfg{child, o.monitor_ms, &mon_stop});

        if (o.kill_after_s > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(o.kill_after_s));
            std::cout << "[parent] SIGKILL child pid=" << child << "\n";
            std::cout.flush();
            ::kill(child, SIGKILL);
        }

        int status = 0;
        ::waitpid(child, &status, 0);

        mon_stop.store(true, std::memory_order_relaxed);
        mon.join();

        if (WIFSIGNALED(status)) {
            std::cout << "[parent] child died by signal " << WTERMSIG(status) << "\n";
        } else if (WIFEXITED(status)) {
            std::cout << "[parent] child exit code " << WEXITSTATUS(status) << "\n";
        }
        std::cout.flush();
    }
    return 0;
}

int main(int argc, char** argv) {
    try {
        Opt o = parse(argc, argv);
        if (o.mode == "threads") return threads_mode(o);
        if (o.mode == "fork")    return fork_mode(o);
        throw std::runtime_error("bad --mode (threads|fork)");
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        std::cerr << "use --help\n";
        return 1;
    }
}
