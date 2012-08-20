shell_scripts = ceph-hdexco ceph-hdexd

bin_SCRIPTS = $(shell_scripts)
CLEANFILES = $(shell_scripts)

all: $(shell_scripts)

# shell scripts

editpaths = sed \
	-e 's|@bindir[@]|$(bindir)|g' \
	-e 's|@libdir[@]|$(libdir)|g' \
	-e 's|@sysconfdir[@]|$(sysconfdir)|g' \
	-e 's|@datadir[@]|$(pkgdatadir)|g' \
	-e 's|@prefix[@]|$(prefix)|g' \
	-e 's|@@GCOV_PREFIX_STRIP[@][@]|$(GCOV_PREFIX_STRIP)|g'

$(shell_scripts): %: %.in
	rm -f $@ $@.tmp
	$(editpaths) '$(srcdir)/$@.in' >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@
