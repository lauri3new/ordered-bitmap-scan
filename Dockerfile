# ==============================================================================
# Builder stage: compile the extension against PG 17
# ==============================================================================
FROM rust:latest AS builder

# Build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libclang-dev \
    clang \
    build-essential \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    libxml2-utils \
    xsltproc \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-pgrx
RUN cargo install cargo-pgrx --version 0.17.0 --locked

# Initialize pgrx with PG 18
RUN cargo pgrx init --pg18 download

# Copy project source
WORKDIR /build
COPY Cargo.toml Cargo.lock* bitmap_index_filter.control ./
COPY src/ src/
COPY sql/ sql/

# Build and install the extension into the pgrx-managed PG instance
RUN cargo pgrx install --release \
    --pg-config "$(find /root/.pgrx -path '*/18.*/pgrx-install/bin/pg_config' | head -1)"


# Stage extension files for the runtime image
RUN PG_CONFIG="$(find /root/.pgrx -path '*/18.*/pgrx-install/bin/pg_config' | head -1)" && \
    mkdir -p /staging/extension /staging/lib && \
    cp "$($PG_CONFIG --sharedir)/extension/bitmap_index_filter"* /staging/extension/ && \
    cp "$($PG_CONFIG --pkglibdir)/bitmap_index_filter"* /staging/lib/

# ==============================================================================
# Runtime stage: PostgreSQL 18 with extension installed
# ==============================================================================
FROM postgres:18

# Copy the staged extension files from builder
COPY --from=builder /staging/extension/ /usr/share/postgresql/18/extension/
COPY --from=builder /staging/lib/ /usr/lib/postgresql/18/lib/

# Verify extension files are in place
RUN ls /usr/share/postgresql/18/extension/bitmap_index_filter* && \
    ls /usr/lib/postgresql/18/lib/bitmap_index_filter*
