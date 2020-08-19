wasm:
	wasm-pack build -t nodejs --no-typescript \
		--out-dir target/pkg \
		-- --features wasm --no-default-features
	cp target/pkg/eyros_bg.wasm pkg/eyros.wasm
	node pkg/bin/fix.js target/pkg/eyros.js > pkg/eyros.js
