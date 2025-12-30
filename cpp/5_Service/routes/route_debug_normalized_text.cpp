#include "routes.h"
#include "route_utils.h"

#include <fstream>

#include "text_common.h"

void register_route_debug_normalized_text(httplib::Server& app, ServiceRouteContext& ctx) {
  // GET /v1/orgs/{org}/debug/normalized_text?name=...&normalize=1&max_bytes=1000
  app.Get(R"(/v1/orgs/([^/]+)/debug/normalized_text)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.has_param("name")) {
        reply_json(res, 400, {{"error","missing name param"}});
        return;
      }
      const std::string name = req.get_param_value("name");
      if (!is_safe_upload_name(name)) {
        reply_json(res, 400, {{"error","bad name"}});
        return;
      }

      bool normalize = true;
      if (req.has_param("normalize")) {
        const std::string v = to_lower_copy(req.get_param_value("normalize"));
        if (v == "1" || v == "true" || v == "yes" || v == "on") normalize = true;
        else if (v == "0" || v == "false" || v == "no" || v == "off") normalize = false;
        else { reply_json(res, 400, {{"error","bad normalize param"}}); return; }
      }

      size_t max_bytes = 1000;
      if (req.has_param("max_bytes")) {
        try { max_bytes = (size_t)std::stoull(req.get_param_value("max_bytes")); }
        catch (...) { reply_json(res, 400, {{"error","bad max_bytes"}}); return; }
      }
      const size_t HARD_CAP = 8ull * 1024 * 1024; // 8 MiB
      if (max_bytes == 0 || max_bytes > HARD_CAP) max_bytes = HARD_CAP;

      const fs::path uploads_dir = ctx.data_root / "orgs" / org_id / "uploads";
      const fs::path src_path = uploads_dir / name;

      std::error_code ec;
      if (!fs::exists(src_path, ec) || ec) {
        reply_json(res, 404, {{"error","file not found"}, {"path", src_path.string()}});
        return;
      }

      const std::string ext = lower_ext(src_path);

      std::string raw_text;
      fs::path used_path = src_path;
      bool converted = false;

      if (ext == ".txt") {
        raw_text = to_text_utf8_best_effort_from_path(src_path, max_bytes);
      } else if (ext == ".doc" || ext == ".docx") {
        const fs::path tmp = mk_tmp_dir_simple("l5_debug_norm");
        const fs::path out_dir = tmp / "out";
        const fs::path profile_dir = tmp / "lo_profile";
        ensure_dirs(out_dir);
        ensure_dirs(profile_dir);

        const fs::path abs_profile = fs::absolute(profile_dir);
        const std::string profile_uri = "file://" + abs_profile.string();

        const std::string cmd =
          "soffice --headless --nologo --nolockcheck --nodefault --norestore"
          " -env:UserInstallation=" + shell_quote(profile_uri) +
          " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
          " --outdir " + shell_quote(out_dir.string()) + " " +
          shell_quote(src_path.string());

        const int rc = run_cmd_bash(cmd);
        if (rc != 0) {
          std::error_code ec2;
          fs::remove_all(tmp, ec2);
          reply_json(res, 500, {{"error","soffice convert failed"}, {"rc", rc}});
          return;
        }

        const fs::path out_txt = out_dir / replace_ext_txt(src_path.filename());
        if (!fs::exists(out_txt, ec)) {
          std::error_code ec2;
          fs::remove_all(tmp, ec2);
          reply_json(res, 500, {{"error","converted txt not found"}, {"path", out_txt.string()}});
          return;
        }

        used_path = out_txt;
        converted = true;

        raw_text = to_text_utf8_best_effort_from_path(out_txt, max_bytes);

        std::error_code ec2;
        fs::remove_all(tmp, ec2);
      } else {
        reply_json(res, 400, {{"error","unsupported file type"}, {"ext", ext}});
        return;
      }

      std::string out_text;
      if (normalize) normalize_for_shingles_simple_to(raw_text, out_text);
      else out_text = std::move(raw_text);

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"name", name},
        {"path", src_path.string()},
        {"used_path", used_path.string()},
        {"converted", converted ? 1 : 0},
        {"normalize", normalize ? 1 : 0},
        {"max_bytes", (uint64_t)max_bytes},
        {"text", out_text}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
