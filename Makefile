wasm:
	@if test -n "$(n)"; then \
		make -s wasm-n n="$(n)"; \
	else \
		for n in 2d 3d 4d 5d 6d 7d 8d; do \
			echo "[BUILD] $$n"; \
			make -s wasm-n n="$$n"; \
		done; \
	fi

wasm-dev:
	@if test -n "$(n)"; then \
		make -s wasm-n n="$(n)"; \
	else \
		for n in 2d 3d 4d 5d 6d 7d 8d; do \
			echo "[BUILD] $$n"; \
			make -s wasm-n-dev n="$$n"; \
		done; \
	fi

wasm-n:
	wasm-pack build -t nodejs --no-typescript \
		--out-dir target/pkg \
		-- --no-default-features --features wasm --features $(n)
	cp target/pkg/eyros_bg.wasm pkg/$(n).wasm
	node pkg/bin/fix.js target/pkg/eyros.js $(n).wasm > pkg/lib/$(n)-api.js

wasm-n-dev:
	wasm-pack build --dev -t nodejs --no-typescript \
		--out-dir target/pkg \
		-- --no-default-features --features wasm --features $(n)
	cp target/pkg/eyros_bg.wasm pkg/$(n).wasm
	node pkg/bin/fix.js target/pkg/eyros.js $(n).wasm > pkg/lib/$(n)-api.js

clean:
	@for n in 2d 3d 4d 5d 6d 7d 8d; do \
		make -s clean-wasm-n n="$$n"; \
	done

clean-wasm-n:
	@test -e pkg/$(n)-api.js && rm pkg/$(n)-api.js && echo "removed $(n)-api.js" || true
	@test -e pkg/$(n).wasm && rm pkg/$(n).wasm && echo "removed $(n).wasm" || true
