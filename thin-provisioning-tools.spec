#
# Copyright (C) 2011 Red Hat, Inc
#
Summary: Device-mapper thin provisioning tools
Name: device-mapper-persistent-data
Version: 0.0.1
Release: 1%{?dist}
License: GPLv3
Group: System Environment/Base
URL: http://sources.redhat.com/lvm2
BuildRequires: expat-devel, libstdc++-devel, boost-devel
Source0: ftp://sources.redhat.com/pub/lvm2/thin-provisioning-tools-%{version}.tar.bz2
Requires: expat

%description
thin-provisioning-tools contains dump,restore and repair tools to
manage device-mapper thin provisioning target metadata devices.

%prep
%setup -q -n thin-provisioning-tools-%{version}

%build
%global _root_sbindir /sbin
%configure --enable-debug --enable-testing

%install
make DESTDIR=%{buildroot} install
mkdir -p %{buildroot}%{_mandir}/man8/
install -c -m644 man8/* %{buildroot}%{_mandir}/man8/

%clean

%files
%doc COPYING README
%{_mandir}/man8/thin_dump.8.gz
%{_mandir}/man8/thin_repair.8.gz
%{_mandir}/man8/thin_restore.8.gz
%{_root_sbindir}/thin_dump
%{_root_sbindir}/thin_repair
%{_root_sbindir}/thin_restore

%changelog
* Thu Dec 15 2011  Heinz Mauelshagen <heinzm@redhat.com> - 0.0.1-1
- Initial version
