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

TOOLS:=\
	cache_check \
	cache_dump \
	cache_metadata_size \
	cache_repair \
	cache_restore \
	cache_writeback \
	thin_check \
	thin_delta \
	thin_dump \
	thin_ls \
	thin_repair \
	thin_restore \
	thin_rmap \
	thin_metadata_size \
	thin_metadata_pack \
	thin_metadata_unpack \
	thin_trim \
	era_check \
	era_dump \
	era_invalidate \
	era_restore

MANPAGES:=$(patsubst %,man8/%.8,$(TOOLS))

install: $(PDATA_TOOLS) $(MANPAGES)
	$(INSTALL_DIR) $(BINDIR)
	$(INSTALL_PROGRAM) $(PDATA_TOOLS) $(BINDIR)
	$(STRIP) $(BINDIR)/pdata_tools
	ln -s -f pdata_tools $(BINDIR)/cache_check
	ln -s -f pdata_tools $(BINDIR)/cache_dump
	ln -s -f pdata_tools $(BINDIR)/cache_metadata_size
	ln -s -f pdata_tools $(BINDIR)/cache_repair
	ln -s -f pdata_tools $(BINDIR)/cache_restore
	ln -s -f pdata_tools $(BINDIR)/cache_writeback
	ln -s -f pdata_tools $(BINDIR)/thin_check
	ln -s -f pdata_tools $(BINDIR)/thin_delta
	ln -s -f pdata_tools $(BINDIR)/thin_dump
	ln -s -f pdata_tools $(BINDIR)/thin_ls
	ln -s -f pdata_tools $(BINDIR)/thin_repair
	ln -s -f pdata_tools $(BINDIR)/thin_restore
	ln -s -f pdata_tools $(BINDIR)/thin_rmap
	ln -s -f pdata_tools $(BINDIR)/thin_metadata_size
	ln -s -f pdata_tools $(BINDIR)/thin_metadata_pack
	ln -s -f pdata_tools $(BINDIR)/thin_metadata_unpack
	ln -s -f pdata_tools $(BINDIR)/thin_trim
	ln -s -f pdata_tools $(BINDIR)/era_check
	ln -s -f pdata_tools $(BINDIR)/era_dump
	ln -s -f pdata_tools $(BINDIR)/era_invalidate
	ln -s -f pdata_tools $(BINDIR)/era_restore
	$(INSTALL_DIR) $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_check.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_dump.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_metadata_size.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_repair.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_restore.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/cache_writeback.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_check.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_delta.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_dump.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_ls.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_repair.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_restore.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_rmap.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_metadata_size.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_metadata_pack.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_metadata_unpack.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/era_check.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/era_dump.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/era_restore.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/era_invalidate.8 $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_trim.8 $(MANPATH)/man8

.PHONY: install
