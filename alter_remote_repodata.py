#!/usr/bin/env python

import os
import librepo
import createrepo_c as cr
import optparse
import shutil
import tempfile

# load_contentstat
def download_remote_repodata(url, dest=None):
    repodata_dir = "repodata"
    if not dest: # default
        dest = tempfile.mkdtemp(prefix="alter_remote_repodata")

    abs_dest = os.path.normpath(os.path.join(dest, repodata_dir))
    if os.path.exists(abs_dest):
        shutil.rmtree(abs_dest)
    h = librepo.Handle()
    r = librepo.Result()

    h.setopt(librepo.LRO_URLS, [url],)
    h.setopt(librepo.LRO_REPOTYPE, librepo.LR_YUMREPO)
    h.setopt(librepo.LRO_INTERRUPTIBLE, True)
    h.setopt(librepo.LRO_DESTDIR, os.path.normpath(dest))
    h.setopt(librepo.LRO_CHECKSUM, True)
    h.perform(r)
    assert os.path.exists(abs_dest), "%s does not exist." % abs_dest
    print ("Using tempdir %s" % abs_dest)
    return abs_dest

def get_checksum_type(c_type):
    assert c_type.upper() in ("MD5", "SHA", "SHA1", "SHA224", "SHA256", "SHA384", "SHA512")
    return getattr(cr, c_type.upper(), cr.CHECKSUM_UNKNOWN)

def get_compression_type(c_type):
    assert c_type.upper() in ("GZ", "BZ2", "XZ")
    return getattr(cr, c_type.upper(), cr.NO_COMPRESSION)

def alter_local_repodata(path, dest, opts):
    """
    we want to point to remote repodata aka --baseurl
    """
    repodata_path = os.path.join(dest, "repodata")
    if os.path.exists(repodata_path):
        shutil.rmtree(repodata_path)

    os.makedirs(repodata_path)

    repomd_path  = os.path.join(repodata_path, "repomd.xml")
    pri_xml_path = os.path.join(repodata_path, "primary.xml.gz")
    fil_xml_path = os.path.join(repodata_path, "filelists.xml.gz")
    oth_xml_path = os.path.join(repodata_path, "other.xml.gz")
    pri_db_path  = os.path.join(repodata_path, "primary.sqlite")
    fil_db_path  = os.path.join(repodata_path, "filelists.sqlite")
    oth_db_path  = os.path.join(repodata_path, "other.sqlite")

    pri_xml_cs = cr.ContentStat(get_checksum_type(opts.checksum))
    fil_xml_cs = cr.ContentStat(get_checksum_type(opts.checksum))
    oth_xml_cs = cr.ContentStat(get_checksum_type(opts.checksum))

    pri_xml = cr.PrimaryXmlFile(pri_xml_path, get_compression_type(opts.compression), pri_xml_cs)
    fil_xml = cr.FilelistsXmlFile(fil_xml_path, get_compression_type(opts.compression), fil_xml_cs)
    oth_xml = cr.OtherXmlFile(oth_xml_path, get_compression_type(opts.compression), oth_xml_cs)
    pri_db  = cr.PrimarySqlite(pri_db_path)
    fil_db  = cr.FilelistsSqlite(fil_db_path)
    oth_db  = cr.OtherSqlite(oth_db_path)

    # Load packages from old repodata
    path = os.path.normpath(path) # source repodata
    source_md = cr.Metadata()
    source_md.locate_and_load_xml(path)
    for key in source_md.keys():
        pkg = source_md.get(key)
        pkg.location_base = opts.url # still point to the remote data
        pri_xml.add_pkg(pkg)
        fil_xml.add_pkg(pkg)
        oth_xml.add_pkg(pkg)
        pri_db.add_pkg(pkg)
        fil_db.add_pkg(pkg)
        oth_db.add_pkg(pkg)

    pri_xml.close()
    fil_xml.close()
    oth_xml.close()

    repomd = cr.Repomd()
    repomdrecords = [("primary",  pri_xml_path, pri_xml_cs, pri_db),
                 ("filelists",    fil_xml_path, fil_xml_cs, fil_db),
                 ("other",        oth_xml_path, oth_xml_cs, oth_db),
                 ("primary_db",   pri_db_path,  None, None),
                 ("filelists_db", fil_db_path,  None, None),
                 ("other_db",     oth_db_path,  None, None),]


    # append comps if any
    source_ml = cr.MetadataLocation(path, 1)
    if source_ml["group"]:
        shutil.copy(os.path.join(path, source_ml["group"]), repodata_path)
        comps_xml = cr.RepomdRecord("group", os.path.join(repodata_path, os.path.basename(source_ml["group"])))
        comps_xml_path = comps_xml.location_href
        comps_xml_gz = comps_xml.compress_and_fill(get_checksum_type(opts.checksum), get_compression_type(opts.compression))
        comps_xml_gz_path = comps_xml_gz.location_href

        repomdrecords.extend((("group", comps_xml_path, None, None), ("group_gz", comps_xml_gz_path, None, None)))

    for name, path, cs, db_to_update in repomdrecords:
        record = cr.RepomdRecord(name, path)
        if cs:
            record.load_contentstat(cs)

        # XXX hack
        if path.endswith(".sqlite") and opts.sqlite_compression:
            record = record.compress_and_fill(get_checksum_type(opts.checksum), get_compression_type(opts.sqlite_compression))
        else:
            record.fill(get_checksum_type(opts.checksum))
        if (db_to_update):
            db_to_update.dbinfo_update(record.checksum)
            db_to_update.close()
        repomd.set_record(record)

    open(repomd_path, "w").write(repomd.xml_dump())
    return repodata_path

def main():
    parser = optparse.OptionParser()
    parser.add_option("--url", help="http://path/to/repo")
    parser.add_option("--dest", help="Destination for altered repodata (default=$cwd)", default=os.getcwd())
    parser.add_option("--compression", default="gz")
    parser.add_option("--checksum", default="md5")
    parser.add_option("--sqlite-compression", default="bz2")
    opts, args = parser.parse_args()

    repodata_path = os.path.dirname(download_remote_repodata(opts.url)) # get rid of /repodata
    new_repodata_path = alter_local_repodata(repodata_path, opts.dest, opts)
    shutil.rmtree(repodata_path) # created by mkdtemp
    print new_repodata_path

if __name__ == "__main__":
    main()

