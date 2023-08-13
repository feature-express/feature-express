import json
import re

def bump_version(version):
    major, minor, patch = map(int, version.split('.'))
    return f"{major}.{minor}.{patch + 1}"

def update_file(file_path, pattern, replacement):
    with open(file_path, 'r') as file:
        content = file.read()
    content = re.sub(pattern, replacement, content)
    with open(file_path, 'w') as file:
        file.write(content)

def main():
    config = {
       "cargo_toml_files": [
            "fexpress-main/Cargo.toml",
            "fexpress-main/fexpress-derive/Cargo.toml",
            "fexpress-py/Cargo.toml"
       ],
       "setup_py_file": "fexpress-py/setup.py",
       "version_file": "version"
    }
    with open(config["version_file"], "r") as version_file:
        old_version = version_file.read().strip()

    new_version = bump_version(old_version)

    # Update version file
    with open(config["version_file"], "w") as version_file:
        version_file.write(new_version)

    # Update Cargo.toml files
    for cargo_toml in config["cargo_toml_files"]:
        update_file(cargo_toml, f'version = "{old_version}"', f'version = "{new_version}"')
        print(f"Updated {cargo_toml}")

    # Update setup.py file
    update_file(config["setup_py_file"], f'version="{old_version}"', f'version="{new_version}"')
    print(f"Updated {config['setup_py_file']}")

    print("Version bump completed successfully.")

if __name__ == "__main__":
    main()
