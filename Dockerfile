# Use an official Rust image as the parent image
FROM rust:latest AS build

# Set the current working directory in the container
WORKDIR /app

# Copy the local package files to the container
COPY ./Cargo.toml ./Cargo.toml

# Copy other necessary files
COPY ./requirements-dev.txt ./requirements-dev.txt

# Install Python, pip, and Maturin
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]

# Create a virtual environment and install requirements
RUN python3 -m venv .venv && \
    source .venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r requirements-dev.txt

# Make sure to activate the virtual environment on any subsequent commands
SHELL ["/bin/bash", "-c", "source .venv/bin/activate && exec $0 $*"]

# Copy the whole project
COPY . .

# The default command to run when starting the container
CMD ["make"]

