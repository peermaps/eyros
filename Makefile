wasm:
	wasm-pack build -t nodejs --no-typescript \
		--out-dir target/pkg \
		-- --features wasm --no-default-features
	cp target/pkg/*.wasm pkg/
	node pkg/fix.js target/pkg/eyros.js > pkg/eyros.js
