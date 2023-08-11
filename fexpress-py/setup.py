from setuptools import setup
from setuptools_rust import RustExtension


setup(
    name="fexpress",
    version="0.1.0",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Rust",
    ],
    packages=["fexpress_python", "fexpress_python.sdk"],
    rust_extensions=[RustExtension("fexpress.fexpress", "Cargo.toml", debug=False)],
    include_package_data=True,
    zip_safe=False,
)