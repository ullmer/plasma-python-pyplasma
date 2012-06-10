from distutils.core import setup, Command
from distutils.command.build import build
import os, sys, unittest, subprocess

import plasma.const

def read(fname):
    return file(os.path.join(os.path.dirname(__file__), fname)).read()

class test(Command):
    description = "run tests"
    user_options = [
        ('tests=', 't', "comma-separated list of packages that contain test modules"),
        ]

    boolean_options = []

    def initialize_options(self):
        self.build_base = 'build'
        self.use_resources = ""
        self.refcounts = False
        self.tests = ".test"

    def finalize_options(self):
        if self.refcounts and not hasattr(sys, "gettotalrefcount"):
            raise DistutilsOptionError("refcount option requires Python debug build")
        self.tests = self.tests.split(",")
        self.use_resources = self.use_resources.split(",")

    def run(self):
        self.run_command('build')
        ts = unittest.TestLoader().loadTestsFromNames(['loam.tests', 'plasma.tests'])
        unittest.TextTestRunner().run(ts)
        #for name in self.tests:
        #    package = __import__(name, globals(), locals(), ['*'])
        #    print "Testing package", name, (sys.version, sys.platform, os.name)
        #    ctypeslib.test.run_tests(package,
        #                             "test_*.py",
        #                             self.verbose,
        #                             self.refcounts)


class doc(Command):
    description = "build html documentation"
    user_options = []
    boolean_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        self.run_command('cbuild')
        os.chdir('doc')
        try:
            subprocess.call(['make', 'html'])
            subprocess.call(['make', 'platform'])
            os.chdir('..')
        except:
            os.chdir('..')
            raise

class cbuild(Command):
    description = "build ctypes constants"
    user_options = []
    boolean_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        symbols = ['SEM_UNDO', 'IPC_CREAT', 'IPC_EXCL', 'IPC_NOWAIT',
                   'IPC_RMID', 'IPC_SET', 'IPC_STAT', 'GETVAL', 'SETVAL',
                   'GETPID', 'GETNCNT', 'GETZCNT', 'GETALL', 'SETALL',
                   'EACCES', 'EEXIST', 'EINVAL', 'ENOENT', 'ENOSPC', 'EINTR',
                   'EPERM', 'ERANGE', 'E2BIG', 'EAGAIN', 'EFBIG', 'EIDRM']
        types = ['time_t', 'key_t', 'uid_t', 'gid_t', 'mode_t']
        sizes = ['char', 'short', 'int', 'long', 'long long']
        fh = file('make_const.c', 'w')
        fh.write("#include <sys/types.h>\n")
        fh.write("#include <sys/ipc.h>\n")
        fh.write("#include <sys/sem.h>\n")
        fh.write("#include <errno.h>\n")
        fh.write("#include <stdio.h>\n")
        fh.write("int main(int argc, char *argv) {\n")
        for size in sizes:
            fh.write("  %s %s_val = 1.1;\n" % (size, size.replace(' ', '')))
        for typ in types:
            fh.write("  %s %s_val = 0;\n" % (typ, typ))
            fh.write("  %s %s_val_n = 0;\n" % (typ, typ))
            fh.write("  %s %s_val_f = 0;\n" % (typ, typ))
        for typ in types:
            fh.write("  %s_val_n = %s_val - 1;\n" % (typ, typ))
            fh.write("  %s_val_f = %s_val + 1.1;\n" % (typ, typ))
        fh.write("  printf(\"import ctypes\\n\");\n")
        for sym in symbols:
            fh.write("  printf(\"%s = %%d\\n\", %s);\n" % (sym, sym))
        for typ in types:
            for i in range(len(sizes)):
                xif = "} else if"
                if i == 0:
                    xif = "if"
                fh.write("  %s(sizeof(%s) == sizeof(%s) && %s_val_f == %s_val) {\n" % (xif, typ, sizes[i], typ, sizes[i].replace(' ', '')))
                fh.write("    if(%s_val_n < 0) {\n" % typ)
                fh.write("      printf(\"%s = ctypes.c_%s\\n\");\n" % (typ, sizes[i].replace(' ', '')))
                fh.write("    } else {\n")
                fh.write("      printf(\"%s = ctypes.c_u%s\\n\");\n" % (typ, sizes[i].replace(' ', '')))
                fh.write("    }\n")
            fh.write("  }\n")
        fh.write("  return 0;\n")
        fh.write("}\n")
        fh.close()
        cc = os.getenv('CC', 'gcc')
        subprocess.call([cc, '-o', 'make_const', 'make_const.c'])
        fh = file('plasma/sem_ops/const.py', 'w')
        subprocess.call(['./make_const',], stdout=fh)
        fh.close()
        os.remove('make_const')
        os.remove('make_const.c')

class my_build(build):
    def run(self):
        self.run_command('cbuild')
        build.run(self)

setup(
    name             = "pyplasma",
    version          = plasma.const.PYPLASMA_VERSION,
    author           = "Mmmm Mmmmmm",
    author_email     = "vvvvvvvv@oooooo.com",
    description      = "Pure Python implementation of oblong's plasma messaging protocol",
    license          = "MIT",
    keywords         = "plasma loam oblong g-speak spatial operating environment",
    url              = "http://platform.oblong.com/download/python",
    packages         = ['loam', 'plasma', 'plasma.slaw', 'plasma.sem_ops', 'plasma.pool', 'plasma.pool.mmap', 'plasma.hose', 'plasma.zeroconf'],
    scripts          = ['scripts/pyp-await',
                        'scripts/pyp-create',
                        'scripts/pyp-deposit',
                        'scripts/pyp-info',
                        'scripts/pyp-list',
                        'scripts/pyp-newest-idx',
                        'scripts/pyp-nth',
                        'scripts/pyp-oldest-idx',
                        'scripts/pyp-rename',
                        'scripts/pyp-resize',
                        'scripts/pyp-show-conf',
                        'scripts/pyp-sleep',
                        'scripts/pyp-stop',
                        'scripts/py-pool-tcp-server',
                        'scripts/pypeek',
                        'scripts/pypogo',
                        'scripts/pypoke',
                        'scripts/pyplasma-benchmark'],
    long_description = read('README'),
    classifiers      = ['Development Status :: 2 - Pre-Alpha',
                        'Environment :: Console',
                        'Intended Audience :: Developers',
                        'Intended Audience :: Information Technology',
                        'Intended Audience :: Science/Research',
                        'Intended Audience :: Telecommunications Industry',
                        'License :: Free for non-commercial use',
                        'Topic :: Communications',
                        'Topic :: Software Development :: User Interfaces',
    ],
    cmdclass = { 'test': test, 'cbuild': cbuild, 'build': my_build, 'doc': doc },
)


