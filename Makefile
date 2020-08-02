wasm:
	wasm-pack build -t nodejs --no-typescript \
		--out-dir target/pkg \
		-- --features wasm --no-default-features
	cp target/pkg/eyros.js target/pkg/eyros_bg.wasm pkg/
	node pkg/fix.js pkg/eyros.js
