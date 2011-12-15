#
# Copyright (C)  2011 Red Hat GmbH. All rights reserved.
#
# See file LICENSE at the top of this source tree for license information.
#

Summary: Device-mapper thin provisioning tools
Name: thin-provisioning-tools
Version: 1.0.0
Release: 1%{?dist}
License: GPLv2+
Group: System Environment/Base
URL: http://sources.redhat.com/lvm2
BuildRequires: expat-devel, libstdc++-devel
Source0: ftp://sources.redhat.com/pub/lvm2/thin-provisoning-tools.%{version}.tgz
Requires: expat, libstdc++

%description
thin-provisioning-tools contains dump,restore and repair tools to
manage device-mapper thin provisioning target metadata devices.

%package -n dmraid-events-logwatch
Summary: dmraid logwatch-based email reporting
Group: System Environment/Base
Requires: dmraid-events = %{version}-%{release}, logwatch, /etc/cron.d

%description -n dmraid-events-logwatch
Provides device failure reporting via logwatch-based email reporting.
Device failure reporting has to be activated manually by activating the 
/etc/cron.d/dmeventd-logwatch entry and by calling the dmevent_tool
(see manual page for examples) for any active RAID sets.

%prep
%setup -q -n thin-provisioning-tools.%{version}

%build
%define _exec_prefix ""
%define _bindir /bin
%define _sbindir /usr/sbin
%define _libdir /%{_lib}

%configure --enable-testing

%install
make install

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%doc COPYING COPYING.LIB INSTALL README VERSION WHATS_NEW
/%{_mandir}/man8/thin_dump.8.gz
/%{_mandir}/man8/thin_repair.8.gz
/%{_mandir}/man8/thin_restore.8.gz
%{_sbindir}/thin_dump
%{_sbindir}/thin_repair
%{_sbindir}/thin_restore

%changelog
* Thu Dec 15 2011  Heinz Mauelshagen <heinzm@redhat.com> - 1.0.0-1
- Initial version
