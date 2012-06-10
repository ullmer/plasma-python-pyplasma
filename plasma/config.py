import os, platform, tempfile

IS_WINDOWS = platform.system() == 'Windows'
IS_MACOS = platform.system() == 'Darwin'
IS_64BIT = platform.architecture()[0] == '64bit'

def bit_width():
    if IS_64BIT:
        return 64
    return 32

def os_version():
    if IS_WINDOWS:
        return platform.win32_ver()[0]
    if IS_MACOS:
        return platform.mac_ver()[0]
    if IS_LINUX:
        return platform.linux_distribution()[1]
    return platform.version()

def kernel_version():
    if IS_WINDOWS:
        return platform.version()
    if IS_MACOS:
        return platform.release()
    return platform.uname()[3]

def libc_version():
    v = platform.libc_ver()
    if v[1] == '':
        return 'unknown'
    return v[1]

def yobuild_version():
    return 'unknown'

def system_gspeak_dir():
    if IS_WINDOWS:
        return os.path.join(os.getenv('PROGRAMFILES'), 'oblong')
    for v in ('64-2', '32-2', '64-1', '32-1'):
        p = os.path.join(os.path.sep, 'opt', 'oblong', 'g-speak-%s' % v)
        if os.path.exists(p):
            return p
    if IS_MACOS:
        return '/opt/oblong/g-speak-32-2'
    return '/opt/oblong/g-speak-64-2'

def user_gspeak_dir():
    if IS_MACOS:
        return os.path.join(os.getenv('HOME'), 'Library', 'Application Support', 'oblong')
    if IS_WINDOWS:
        return os.path.join(os.getenv('HOME'), 'oblong')
    return os.path.join(os.getenv('HOME'), '.oblong')

def __root_dir():
    if IS_WINDOWS:
        return os.path.join(os.getenv('SYSTEMDRIVE', 'C:'), os.path.sep)
    return os.path.sep

def ob_prefix_dir():
    return system_gspeak_dir()

def ob_share_path():
    pth = os.getenv('OB_SHARE_PATH', None)
    if pth is None:
        pth = [os.path.join(user_gspeak_dir(), 'share')]
        if IS_WINDOWS:
            pth.append(os.path.join(system_gspeak_dir(), 'share'))
        else:
            pth.append(os.path.join(system_gspeak_dir(), 'share', 'oblong'))
        return pth
    return pth.split(os.path.pathsep)

def ob_etc_path():
    pth = os.getenv('OB_ETC_PATH', None)
    if pth is None:
        pth = [os.path.join(user_gspeak_dir(), 'etc')]
        if IS_WINDOWS:
            pth.append(os.path.join(system_gspeak_dir(), 'etc'))
        else:
            pth.append(os.path.join(os.path.sep, 'etc', 'oblong'))
        return pth
    return pth.split(os.path.pathsep)

def ob_var_path():
    pth = os.getenv('OB_VAR_PATH', None)
    if pth is None:
        pth = [os.path.join(user_gspeak_dir(), 'var')]
        if IS_WINDOWS:
            pth.append(os.path.join(system_gspeak_dir(), 'var'))
        else:
            pth.append(os.path.join(os.path.sep, 'var', 'ob'))
        return pth
    return pth.split(os.path.pathsep)

def ob_tmp_dir():
    return os.getenv('OB_TMP_DIR', os.getenv('TMPDIR', tempfile.gettempdir()))

def ob_pools_dir():
    pth = os.getenv('OB_POOLS_DIR')
    if pth is None:
        if IS_WINDOWS:
            return os.path.join(system_gspeak_dir(), 'var', 'pools')
        return os.path.join(os.path.sep, 'var', 'ob', 'pools')
    return pth

def ob_yobuild_dir():
    pth = os.getenv('YOBUILD')
    if pth is None:
        if IS_WINDOWS:
            return os.path.join(os.getenv('SYSTEMDRIVE'), os.path.sep, 'yobuild')
        if IS_64BIT:
            width = 64
        else:
            width = 32
        yobuild_version_major = 8
        return os.path.join(os.path.sep, 'opt', 'oblong', 'deps-%d-%d' % (width, yobuild_version_major))
    return pth

def config_lock_dir():
    if IS_WINDOWS:
        return os.path.join(os.getenv('SYSTEMDRIVE'), os.path.sep, 'tmp')
    return os.path.join(os.path.sep, 'tmp')

