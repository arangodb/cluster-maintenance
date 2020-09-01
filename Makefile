.PHONY: help readme version dist targz

help:
	@echo "ArangoDB debug scripts"
	@echo
	@echo "make help"
	@echo "  show this help"
	@echo
	@echo "make readme"
	@echo "  show the readme"
	@echo
	@echo "make version"
	@echo "  show the version"
	@echo
	@echo "make dist"
	@echo "  create a distribution tar"

readme:
	@less README.md

version:
	@cat VERSION

dist:
	@mkdir -p work
	$(MAKE) targz V=`git describe --all --tags --long --dirty=-dirty | sed -e 's:tags/::' | sed -e 's:/:_:g'`

targz:
	@echo "generating archive for $V"
	@rm -rf work/debug-scripts-$V
	@mkdir -p work/debug-scripts-$V
	@tar -c -f - \
		CHANGELOG LICENSE Makefile README.md VERSION arangodb-debug.sh \
		`find debugging -name "*.js"` \
		 | tar -C work/debug-scripts-$V -x -f -
	@tar -c -z -f work/debug-scripts-$V.tar.gz -C work debug-scripts-$V
	@mv work/debug-scripts-$V work/debug-scripts
	@tar -c -z -f work/debug-scripts.tar.gz -C work debug-scripts
	@rm -rf work/debug-scripts
