V=@

PDATA_TOOLS:=\
	target/release/pdata_tools

$(PDATA_TOOLS):
	$(V) cargo build --release

PREFIX:=/usr
BINDIR:=$(DESTDIR)$(PREFIX)/sbin
DATADIR:=$(DESTDIR)$(PREFIX)/share
MANPATH:=$(DATADIR)/man

STRIP:=strip
INSTALL:=install
INSTALL_DIR = $(INSTALL) -m 755 -d
INSTALL_PROGRAM = $(INSTALL) -m 755
INSTALL_DATA = $(INSTALL) -p -m 644

.SUFFIXES: .txt .8

%.8: %.txt bin/txt2man
	@echo "    [txt2man] $<"
	@mkdir -p $(dir $@)
	$(V) bin/txt2man -t $(basename $(notdir $<)) \
	-s 8 -v "System Manager's Manual" -r "Device Mapper Tools" $< > $@

.PHONY: clean

clean:
	cargo clean
	$(RM) man8/*.8

# must be kept in-sync with src/bin/pdata_tools.rs
TOOLS:=\
	cache_check \
	cache_dump \
	cache_metadata_size \
	cache_repair \
	cache_restore \
	cache_writeback \
	era_check \
	era_dump \
	era_invalidate \
 	era_repair \
	era_restore \
	thin_check \
	thin_delta \
	thin_dump \
	thin_ls \
	thin_metadata_pack \
	thin_metadata_size \
	thin_metadata_unpack \
	thin_migrate \
	thin_repair \
	thin_restore \
	thin_rmap \
	thin_shrink \
	thin_trim

MANPAGES:=$(patsubst %,man8/%.8,$(TOOLS))

install: $(PDATA_TOOLS) $(MANPAGES)
	$(INSTALL_DIR) $(BINDIR)
	$(INSTALL_PROGRAM) $(PDATA_TOOLS) $(BINDIR)
	$(STRIP) $(BINDIR)/pdata_tools
	for tool in $(TOOLS); do \
		ln -s -f pdata_tools $(BINDIR)/$$tool; \
	done
	$(INSTALL_DIR) $(MANPATH)/man8
	$(INSTALL_DATA) $(MANPAGES) $(MANPATH)/man8

.PHONY: install
