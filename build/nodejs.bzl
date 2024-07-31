load("@aspect_bazel_lib//lib/private:copy_directory_toolchain.bzl", "copy_directory_toolchains_repo")
load("@aspect_bazel_lib//lib/private:copy_to_directory_toolchain.bzl", "copy_to_directory_toolchains_repo")
load("@rules_nodejs//nodejs:repositories.bzl", "node_repositories")
load("@rules_nodejs//nodejs/private:nodejs_repo_host_os_alias.bzl", "nodejs_repo_host_os_alias")
load("@rules_nodejs//nodejs/private:toolchains_repo.bzl", "toolchains_repo")

_NODE_VERSION = "16.14.2"
_VERSIONS = {
    "darwin_amd64": ("node-v16.14.2-darwin-x64.tar.gz", "node-v16.14.2-darwin-x64", "d3076ca7fcc7269c8ff9b03fe7d1c277d913a7e84a46a14eff4af7791ff9d055"),
    "darwin_arm64": ("node-v16.14.2-darwin-arm64.tar.gz", "node-v16.14.2-darwin-arm64", "a66d9217d2003bd416d3dd06dfd2c7a044c4c9ff2e43a27865790bd0d59c682d"),
    "linux_amd64": ("node-v16.14.2-linux-x64.tar.xz", "node-v16.14.2-linux-x64", "e40c6f81bfd078976d85296b5e657be19e06862497741ad82902d0704b34bb1b"),
    "linux_arm64": ("node-v16.14.2-linux-arm64.tar.xz", "node-v16.14.2-linux-arm64", "f7c5a573c06a520d6c2318f6ae204141b8420386553a692fc359f8ae3d88df96"),
    "windows_amd64": ("node-v16.14.2-win-x64.zip", "node-v16.14.2-win-x64", "4731da4fbb2015d414e871fa9118cabb643bdb6dbdc8a69a3ed563266ac93229"),
}

# Versions of copy_directory and copy_to_directory from bazel-lib (github.com/aspect-build/bazel-lib)
_COPY_DIRECTORY_URL_PREFIX = "https://storage.googleapis.com/public-bazel-artifacts/js/aspect-bazel-lib-utils-2024-04-29/copy_directory-"

_COPY_DIRECTORY_VERSIONS = {
    "darwin_amd64": "107e90a5ffc8cc86869dc8a037e70c736c426f40b75ee57c23871406d699ec61",
    "darwin_arm64": "0c7daf978934312ca9fa59ef7e288ebb489f73eb594a025420e16d85238c32f8",
    "linux_amd64": "406148a22bdcd33f766daae4c3f24be0b6e0815f3d9e609fb119032bb7f3e206",
    "linux_arm64": "9525248829a141a4b13cd0da5bc372f9c8a95b57dcbcda205f9131df3375efce",
    "windows_amd64": "8a8014c5c48984c446eed8216510c7fd68c04d41148d5c8d3750acd81028cc9b",
}

_COPY_TO_DIRECTORY_URL_PREFIX = "https://storage.googleapis.com/public-bazel-artifacts/js/aspect-bazel-lib-utils-2024-04-29/copy_to_directory-"

_COPY_TO_DIRECTORY_VERSIONS = {
    "darwin_amd64": "1f415f43721b17d4579743b22e45479f335612d910b8b66af3629362f8437a5e",
    "darwin_arm64": "3372dc06b0aa23966f799a9ea377fbf13449db579b593400fed0ce7c0ba5daad",
    "linux_amd64": "ccd984ed134c4d126aad4db0d380b7b7003734aabb1ef1545a29b61c1c09e0a8",
    "linux_arm64": "5611bf54c941c07c3ebccbfc805251d45758b945dbf3937f0844e611e75f1fb6",
    "windows_amd64": "f8270fb9f4f49c3e1729b6542072b847e28a885cc2d448ebffc4a39e8dda1d1a",
}

# NOTE: This code is adapted from upstream at
# https://github.com/aspect-build/bazel-lib/blob/c89ec6a554321cf4ff0430fd388edefb7f606ccc/lib/private/copy_directory_toolchain.bzl#LL155C1-L188C2
# We can't use the upstream version of this code as written so we need to duplicate some stuff.
def _copy_directory_platform_repo_impl(rctx):
    plat = rctx.attr.platform
    is_windows = "windows" in rctx.attr.platform
    url = _COPY_DIRECTORY_URL_PREFIX + plat + (".exe" if is_windows else "")
    rctx.download(
        url = url,
        output = "copy_directory.exe" if is_windows else "copy_directory",
        executable = True,
        sha256 = _COPY_DIRECTORY_VERSIONS[plat]
    )
    build_content = """# @generated by @com_github_cockroachdb_cockroach//build:nodejs.bzl
load("@aspect_bazel_lib//lib/private:copy_directory_toolchain.bzl", "copy_directory_toolchain")
exports_files(["{0}"])
copy_directory_toolchain(name = "copy_directory_toolchain", bin = "{0}", visibility = ["//visibility:public"])
""".format("copy_directory.exe" if is_windows else "copy_directory")
    # Base BUILD file for this repository
    rctx.file("BUILD.bazel", build_content)

copy_directory_platform_repo = repository_rule(
    implementation = _copy_directory_platform_repo_impl,
    doc = "Fetch external tools needed for copy_directory toolchain",
    attrs = {
        "platform": attr.string(mandatory = True, values = _COPY_DIRECTORY_VERSIONS.keys()),
    },
)

def _copy_to_directory_platform_repo_impl(rctx):
    plat = rctx.attr.platform
    is_windows = "windows" in rctx.attr.platform
    url = _COPY_TO_DIRECTORY_URL_PREFIX + plat + (".exe" if is_windows else "")
    rctx.download(
        url = url,
        output = "copy_to_directory.exe" if is_windows else "copy_to_directory",
        executable = True,
        sha256 = _COPY_TO_DIRECTORY_VERSIONS[plat]
    )
    build_content = """# @generated by @com_github_cockroachdb_cockroach//build:nodejs.bzl
load("@aspect_bazel_lib//lib/private:copy_to_directory_toolchain.bzl", "copy_to_directory_toolchain")
exports_files(["{0}"])
copy_to_directory_toolchain(name = "copy_to_directory_toolchain", bin = "{0}", visibility = ["//visibility:public"])
""".format("copy_to_directory.exe" if is_windows else "copy_to_directory")

    # Base BUILD file for this repository
    rctx.file("BUILD.bazel", build_content)

copy_to_directory_platform_repo = repository_rule(
    implementation = _copy_to_directory_platform_repo_impl,
    doc = "Fetch external tools needed for copy_to_directory toolchain",
    attrs = {
        "platform": attr.string(mandatory = True, values = _COPY_TO_DIRECTORY_VERSIONS.keys()),
    },
)

# Helper function used in WORKSPACE.
# Note this function basically re-implements the functionality of the nodejs_register_toolchains function
# in @rules_js//nodejs:repositories.bzl.
# We do this to have more control over how this stuff is created, to use our
# node mirrors, etc.
def declare_nodejs_repos():
    for name in _VERSIONS:
        node_repositories(
            name = "nodejs_" + name,
            node_repositories = {
                _NODE_VERSION + "-" + name: _VERSIONS[name]
            },
            node_urls = [
                "https://storage.googleapis.com/public-bazel-artifacts/js/node/v{version}/{filename}",
            ],
            node_version = _NODE_VERSION,
            platform = name,
        )
    nodejs_repo_host_os_alias(name = "nodejs", user_node_repository_name = "nodejs")
    nodejs_repo_host_os_alias(name = "nodejs_host", user_node_repository_name = "nodejs")
    toolchains_repo(name = "nodejs_toolchains", user_node_repository_name = "nodejs")
    # NB: npm_import has weird behavior where it transparently makes these
    # copy_directory/copy_to_directory repos for you if you do not set up the
    # repositories beforehand. This is weird behavior that will hopefully be
    # fixed in a later version. For now it's important this function be called
    # before we call into npm_import() in WORKSPACE.
    # Ref: https://github.com/aspect-build/rules_js/blob/a043b6cd1138e608272c2feef8905baf85d86b97/npm/private/npm_import.bzl#L1121
    for plat in _COPY_DIRECTORY_VERSIONS:
        copy_directory_platform_repo(
            name = "copy_directory_" + plat,
            platform = plat,
        )
    for plat in _COPY_TO_DIRECTORY_VERSIONS:
        copy_to_directory_platform_repo(
            name = "copy_to_directory_" + plat,
            platform = plat,
        )
    copy_directory_toolchains_repo(
        name = "copy_directory_toolchains",
        user_repository_name = "copy_directory",
    )
    copy_to_directory_toolchains_repo(
        name = "copy_to_directory_toolchains",
        user_repository_name = "copy_to_directory",
    )
