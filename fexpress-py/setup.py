from setuptools import setup
from setuptools_rust import RustExtension


setup(
    name="fexpress",
    version="0.0.2",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Rust",
    ],
    packages=["fexpress", "fexpress.sdk"],
    rust_extensions=[RustExtension("fexpress.fexpress", "Cargo.toml", debug=False)],
    include_package_data=True,
    zip_safe=False,
)
