# fexpress-rs

### version 0.1.1

This is an implementation of FExpress in Rust with Python binding.

# development

```
env VIRTUAL_ENV=$(python3 -c 'import sys; print(sys.base_prefix)') maturin develop
```

or

```
maturin develop
```

# development (optimized code)

```
maturin develop --release
```

# building Python wheel

```
maturin build --release -i python
```

This should create a wheel in `target/wheels`

# installing Python wheel

```
pip install target/wheels/fexpress_rs-0.1.0-cp38-cp38-linux_x86_64.whl -U
```

Note that the file name can be different depending on your system.



