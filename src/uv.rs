use std::path::{Path, PathBuf};

const UV_VERSION: &str = "0.11.2";

/// Returns the path to a `uv` binary.
///
/// Lookup order:
/// 1. `uv` found on `PATH`
/// 2. Cached binary at `~/.cache/hegel/uv`
/// 3. Downloads uv to `~/.cache/hegel/uv` and returns that path
///
/// Panics if uv cannot be found or downloaded.
pub fn find_uv() -> String {
    resolve_uv(find_in_path("uv"), cached_uv_path(), cache_dir())
}

fn resolve_uv(path_uv: Option<PathBuf>, cached: PathBuf, cache: PathBuf) -> String {
    if let Some(path) = path_uv {
        return path.to_string_lossy().into_owned();
    }
    if cached.is_file() {
        return cached.to_string_lossy().into_owned();
    }
    download_uv_to(&cache).unwrap_or_else(|e| panic!("{e}"));
    cached.to_string_lossy().into_owned()
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|p| p.is_file())
}

fn cache_dir() -> PathBuf {
    cache_dir_from(std::env::var("XDG_CACHE_HOME").ok(), std::env::home_dir())
}

fn cache_dir_from(xdg_cache_home: Option<String>, home_dir: Option<PathBuf>) -> PathBuf {
    if let Some(xdg_cache) = xdg_cache_home {
        return PathBuf::from(xdg_cache).join("hegel");
    }
    let home = home_dir.expect("Could not determine home directory");
    home.join(".cache").join("hegel")
}

fn cached_uv_path() -> PathBuf {
    cache_dir().join("uv")
}

fn platform_archive_name() -> Result<String, String> {
    archive_name_for(std::env::consts::ARCH, std::env::consts::OS)
}

fn archive_name_for(arch: &str, os: &str) -> Result<String, String> {
    let triple = match (arch, os) {
        ("aarch64", "macos") => "aarch64-apple-darwin",
        ("x86_64", "macos") => "x86_64-apple-darwin",
        ("aarch64", "linux") => "aarch64-unknown-linux-musl",
        ("x86_64", "linux") => "x86_64-unknown-linux-musl",
        _ => {
            return Err(format!(
                "Unsupported platform: {arch}-{os}. \
                 Install uv manually: https://docs.astral.sh/uv/getting-started/installation/"
            ));
        }
    };
    Ok(format!("uv-{triple}.tar.gz"))
}

fn download_uv_to(cache: &Path) -> Result<(), String> {
    let archive_name = platform_archive_name()?;
    let url =
        format!("https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/{archive_name}");
    download_url_to_cache(&url, &archive_name, cache)
}

fn download_url_to_cache(url: &str, archive_name: &str, cache: &Path) -> Result<(), String> {
    std::fs::create_dir_all(cache)
        .map_err(|e| format!("Failed to create cache directory {}: {e}", cache.display()))?;

    // Use a per-process temp directory inside the cache dir so that:
    // 1. Concurrent downloads don't interfere with each other
    // 2. The final rename is atomic (same filesystem)
    let temp_dir = cache.join(format!(".uv-download-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir)
        .map_err(|e| format!("Failed to create temp directory: {e}"))?;
    let _cleanup = CleanupGuard(&temp_dir);

    let archive_path = temp_dir.join(archive_name);

    let output = std::process::Command::new("curl")
        .args(["-fsSL", "-o"])
        .arg(&archive_path)
        .arg(url)
        .output()
        .map_err(|e| format!("Failed to run curl to download uv: {e}. Install uv manually: https://docs.astral.sh/uv/getting-started/installation/"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "Failed to download uv from {url}: {stderr}\n\
             Install uv manually: https://docs.astral.sh/uv/getting-started/installation/"
        ));
    }

    let output = std::process::Command::new("tar")
        .args(["xzf"])
        .arg(&archive_path)
        .args(["--strip-components", "1", "-C"])
        .arg(&temp_dir)
        .output()
        .map_err(|e| format!("Failed to extract uv archive: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to extract uv archive: {stderr}"));
    }

    let extracted_uv = temp_dir.join("uv");

    // Atomic rename — safe under concurrent downloads because rename on the
    // same filesystem is atomic on Unix, so the last writer wins with a
    // valid binary.
    let final_path = cache.join("uv");
    std::fs::rename(&extracted_uv, &final_path)
        .map_err(|e| format!("Failed to install uv to {}: {e}", final_path.display()))?;

    Ok(())
}

/// RAII guard that removes a directory on drop.
struct CleanupGuard<'a>(&'a std::path::Path);

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real release archive names from uv 0.11.2 on GitHub.
    /// Source: https://github.com/astral-sh/uv/releases/tag/0.11.2
    const UV_RELEASE_ARCHIVES: &[&str] = &[
        "uv-aarch64-apple-darwin.tar.gz",
        "uv-aarch64-pc-windows-msvc.zip",
        "uv-aarch64-unknown-linux-gnu.tar.gz",
        "uv-aarch64-unknown-linux-musl.tar.gz",
        "uv-arm-unknown-linux-musleabihf.tar.gz",
        "uv-armv7-unknown-linux-gnueabihf.tar.gz",
        "uv-armv7-unknown-linux-musleabihf.tar.gz",
        "uv-i686-pc-windows-msvc.zip",
        "uv-i686-unknown-linux-gnu.tar.gz",
        "uv-i686-unknown-linux-musl.tar.gz",
        "uv-powerpc64le-unknown-linux-gnu.tar.gz",
        "uv-riscv64gc-unknown-linux-gnu.tar.gz",
        "uv-riscv64gc-unknown-linux-musl.tar.gz",
        "uv-s390x-unknown-linux-gnu.tar.gz",
        "uv-x86_64-apple-darwin.tar.gz",
        "uv-x86_64-pc-windows-msvc.zip",
        "uv-x86_64-unknown-linux-gnu.tar.gz",
        "uv-x86_64-unknown-linux-musl.tar.gz",
    ];

    /// The (arch, os) pairs that archive_name_for supports, using the values
    /// that std::env::consts::{ARCH, OS} would return on those platforms.
    const SUPPORTED_PLATFORMS: &[(&str, &str)] = &[
        ("aarch64", "macos"),
        ("x86_64", "macos"),
        ("aarch64", "linux"),
        ("x86_64", "linux"),
    ];

    #[test]
    fn test_all_supported_platforms_have_real_release_archives() {
        for &(arch, os) in SUPPORTED_PLATFORMS {
            let name = archive_name_for(arch, os).unwrap();
            assert!(
                UV_RELEASE_ARCHIVES.contains(&name.as_str()),
                "archive_name_for({arch:?}, {os:?}) = {name:?} is not in the uv release"
            );
        }
    }

    #[test]
    fn test_all_release_archives_are_covered() {
        let supported: Vec<String> = SUPPORTED_PLATFORMS
            .iter()
            .map(|&(arch, os)| archive_name_for(arch, os).unwrap())
            .collect();

        let uncovered: Vec<&&str> = UV_RELEASE_ARCHIVES
            .iter()
            .filter(|name| !supported.contains(&name.to_string()))
            .collect();

        // We only expect to not cover Windows (.zip) and non-musl Linux
        // variants — we don't need to support every platform uv ships for.
        for name in &uncovered {
            assert!(
                name.ends_with(".zip")
                    || name.contains("-gnu")
                    || name.contains("-musleabihf")
                    || name.contains("-i686-")
                    || name.contains("-arm-")
                    || name.contains("-armv7-")
                    || name.contains("-powerpc64le-")
                    || name.contains("-riscv64gc-")
                    || name.contains("-s390x-"),
                "release archive {name} is not covered by archive_name_for and is not \
                 an expected exclusion — should it be added as a supported platform?"
            );
        }
    }

    #[test]
    fn test_unsupported_platform_returns_error() {
        let err = archive_name_for("mips", "freebsd").unwrap_err();
        assert!(err.contains("Unsupported platform: mips-freebsd"));
        assert!(err.contains("Install uv manually"));
    }

    #[test]
    fn test_cache_dir_with_xdg() {
        let result = cache_dir_from(Some("/tmp/xdg".to_string()), None);
        assert_eq!(result, PathBuf::from("/tmp/xdg/hegel"));
    }

    #[test]
    fn test_cache_dir_with_home() {
        let result = cache_dir_from(None, Some(PathBuf::from("/home/test")));
        assert_eq!(result, PathBuf::from("/home/test/.cache/hegel"));
    }

    #[test]
    fn test_find_in_path_finds_known_binary() {
        assert!(find_in_path("sh").is_some());
    }

    #[test]
    fn test_find_in_path_returns_none_for_missing() {
        assert!(find_in_path("definitely_not_a_real_binary_xyz").is_none());
    }

    #[test]
    fn test_resolve_uv_prefers_path() {
        let temp = tempfile::tempdir().unwrap();
        let fake_uv = temp.path().join("uv");
        std::fs::write(&fake_uv, "fake").unwrap();

        let result = resolve_uv(
            Some(fake_uv.clone()),
            PathBuf::from("/nonexistent/uv"),
            PathBuf::from("/nonexistent"),
        );
        assert_eq!(result, fake_uv.to_string_lossy());
    }

    #[test]
    fn test_resolve_uv_uses_cache() {
        let temp = tempfile::tempdir().unwrap();
        let cached = temp.path().join("uv");
        std::fs::write(&cached, "fake").unwrap();

        let result = resolve_uv(None, cached.clone(), PathBuf::from("/nonexistent"));
        assert_eq!(result, cached.to_string_lossy());
    }

    #[test]
    fn test_download_uv_to_creates_binary() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("hegel");
        download_uv_to(&cache).unwrap();
        assert!(cache.join("uv").is_file());
    }

    #[test]
    fn test_resolve_uv_downloads_when_not_cached() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("hegel");
        let cached_uv = cache.join("uv");

        let result = resolve_uv(None, cached_uv.clone(), cache);
        assert_eq!(result, cached_uv.to_string_lossy());
        assert!(cached_uv.is_file());
    }

    #[test]
    fn test_cleanup_guard_removes_directory() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("cleanup-test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("file.txt"), "data").unwrap();
        {
            let _guard = CleanupGuard(&dir);
        }
        assert!(!dir.exists());
    }

    #[test]
    fn test_download_invalid_cache_path() {
        let temp = tempfile::tempdir().unwrap();
        // Create a file where a directory is expected
        let blocker = temp.path().join("blocker");
        std::fs::write(&blocker, "not a directory").unwrap();
        let bad_cache = blocker.join("hegel");

        let err =
            download_url_to_cache("https://example.com/fake.tar.gz", "fake.tar.gz", &bad_cache)
                .unwrap_err();
        assert!(err.contains("Failed to create cache directory"));
    }

    #[test]
    fn test_download_bad_url() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("hegel");

        let err = download_url_to_cache(
            "https://github.com/astral-sh/uv/releases/download/nonexistent/fake.tar.gz",
            "fake.tar.gz",
            &cache,
        )
        .unwrap_err();
        assert!(err.contains("Failed to download uv"));
    }

    #[test]
    fn test_download_invalid_archive() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("hegel");
        std::fs::create_dir_all(&cache).unwrap();

        // Create a fake "archive" that curl would have "downloaded"
        // We'll test tar extraction failure by calling download_url_to_cache
        // with a URL that returns non-tar data
        // Use a URL that returns HTML (not a tar.gz)
        let err = download_url_to_cache("https://example.com", "not-a-real-archive.tar.gz", &cache);
        // curl with -f flag will fail on non-200 or we get a tar extraction error
        assert!(err.is_err());
    }
}
