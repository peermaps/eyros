wasm:
	wasm-pack build -t nodejs --no-typescript -- \
		--features wasm --no-default-features
