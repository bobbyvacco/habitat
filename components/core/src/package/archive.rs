use super::{metadata::{MetaFile,
                       PackageType},
            Identifiable,
            PackageIdent,
            PackageTarget};
use crate::{crypto::{artifact,
                     hash},
            error::{Error,
                    Result}};
use libarchive::{archive::{Entry,
                           ExtractOption,
                           ExtractOptions,
                           ReadFilter,
                           ReadFormat},
                 reader::{self,
                          Reader},
                 writer};
use regex::Regex;
use std::{collections::HashMap,
          error,
          path::{Path,
                 PathBuf},
          result,
          str::{self,
                FromStr},
          string::ToString};

lazy_static::lazy_static! {
    static ref METAFILE_REGXS: HashMap<MetaFile, Regex> = {
        let mut map = HashMap::new();
        map.insert(
            MetaFile::CFlags,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::CFlags
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Config,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Config
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Deps,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Deps
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::TDeps,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::TDeps
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::BuildDeps,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::BuildDeps,
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::BuildTDeps,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::BuildTDeps,
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Exposes,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Exposes
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Ident,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Ident
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::LdRunPath,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::LdRunPath
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::LdFlags,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::LdFlags
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::SvcUser,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::SvcUser
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Services,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Services
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::ResolvedServices,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::ResolvedServices
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Manifest,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Manifest
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Path,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Path
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Target,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Target
            ))
            .unwrap(),
        );
        map.insert(
            MetaFile::Type,
            Regex::new(&format!(
                r"^/?hab/pkgs/([^/]+)/([^/]+)/([^/]+)/([^/]+)/{}$",
                MetaFile::Type
            ))
            .unwrap(),
        );
        map
    };
}

type Metadata = HashMap<MetaFile, String>;

#[derive(Debug)]
pub struct PackageArchive {
    pub path: PathBuf,
    metadata: Option<Metadata>,
}

impl PackageArchive {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        PackageArchive { path:     path.into(),
                         metadata: None, }
    }

    /// Calculate and return the checksum of the package archive in base64 format.
    ///
    /// # Failures
    ///
    /// * If the archive cannot be read
    pub fn checksum(&self) -> Result<String> { hash::hash_file(&self.path) }

    pub fn cflags(&mut self) -> Result<Option<String>> {
        match self.read_metadata(MetaFile::CFlags) {
            Ok(data) => Ok(data.cloned()),
            Err(e) => Err(e),
        }
    }

    pub fn config(&mut self) -> Result<Option<String>> {
        match self.read_metadata(MetaFile::Config) {
            Ok(data) => Ok(data.cloned()),
            Err(e) => Err(e),
        }
    }

    // hab-plan-build.sh only generates SVC_USER and SVC_GROUP files if it thinks a package is
    // a service. It determines that by checking for the presence of a run hook file or a
    // pkg_svc_run value. Therefore, if we can detect the presence of a SVC_USER file, we can
    // consider this archive a service.
    //
    // The allow below is necessary because `is_*` functions expect a `&self`, not `&mut self`.
    // It would be good to refactor this struct to do the read_metadata in new and then
    // eliminate the `&mut self`s on all the accessor functions, but that's a more involved
    // change than we want to undertake now.
    //
    // See https://rust-lang.github.io/rust-clippy/master/index.html#wrong_self_convention
    #[allow(clippy::wrong_self_convention)]
    pub fn is_a_service(&mut self) -> bool {
        match self.svc_user() {
            Ok(_) => true,
            _ => false,
        }
    }

    /// Returns a list of package identifiers representing the runtime package dependencies for
    /// this archive.
    ///
    /// # Failures
    ///
    /// * If the archive cannot be read
    /// * If the archive cannot be verified
    pub fn deps(&mut self) -> Result<Vec<PackageIdent>> { self.read_deps(MetaFile::Deps) }

    /// Returns a list of package identifiers representing the transitive runtime package
    /// dependencies for this archive.
    ///
    /// # Failures
    ///
    /// * If the archive cannot be read
    /// * If the archive cannot be verified
    pub fn tdeps(&mut self) -> Result<Vec<PackageIdent>> { self.read_deps(MetaFile::TDeps) }

    /// Returns a list of package identifiers representing the buildtime package dependencies for
    /// this archive.
    ///
    /// # Failures
    ///
    /// * If the archive cannot be read
    /// * If the archive cannot be verified
    pub fn build_deps(&mut self) -> Result<Vec<PackageIdent>> {
        self.read_deps(MetaFile::BuildDeps)
    }

    /// Returns a list of package identifiers representing the transitive buildtime package
    /// dependencies for this archive.
    ///
    /// # Failures
    ///
    /// * If the archive cannot be read
    /// * If the archive cannot be verified
    pub fn build_tdeps(&mut self) -> Result<Vec<PackageIdent>> {
        self.read_deps(MetaFile::BuildTDeps)
    }

    pub fn exposes(&mut self) -> Result<Vec<u16>> {
        match self.read_metadata(MetaFile::Exposes) {
            Ok(Some(data)) => {
                let ports: Vec<u16> = data.split_whitespace()
                                          .filter_map(|port| port.parse::<u16>().ok())
                                          .collect();
                Ok(ports)
            }
            Ok(None) => Ok(vec![]),
            Err(e) => Err(e),
        }
    }

    pub fn ident(&mut self) -> Result<PackageIdent> {
        match self.read_metadata(MetaFile::Ident) {
            Ok(None) => Err(Error::MetaFileNotFound(MetaFile::Ident)),
            Ok(Some(data)) => PackageIdent::from_str(&data),
            Err(e) => Err(e),
        }
    }

    pub fn ld_run_path(&mut self) -> Result<Option<String>> {
        match self.read_metadata(MetaFile::LdRunPath) {
            Ok(data) => Ok(data.cloned()),
            Err(e) => Err(e),
        }
    }

    pub fn ldflags(&mut self) -> Result<Option<String>> {
        match self.read_metadata(MetaFile::LdFlags) {
            Ok(data) => Ok(data.cloned()),
            Err(e) => Err(e),
        }
    }

    pub fn svc_user(&mut self) -> Result<String> {
        match self.read_metadata(MetaFile::SvcUser) {
            Ok(Some(data)) => Ok(data.clone()),
            Ok(None) => Err(Error::MetaFileNotFound(MetaFile::SvcUser)),
            Err(e) => Err(e),
        }
    }

    pub fn manifest(&mut self) -> Result<String> {
        match self.read_metadata(MetaFile::Manifest) {
            Ok(None) => Err(Error::MetaFileNotFound(MetaFile::Manifest)),
            Ok(Some(data)) => Ok(data.clone()),
            Err(e) => Err(e),
        }
    }

    pub fn package_type(&mut self) -> Result<PackageType> {
        match self.read_metadata(MetaFile::Type) {
            Ok(None) => Ok(PackageType::Standalone),
            Ok(Some(data)) => PackageType::from_str(&data),
            Err(e) => Err(e),
        }
    }

    pub fn path(&mut self) -> Result<Option<String>> {
        match self.read_metadata(MetaFile::Path) {
            Ok(data) => Ok(data.cloned()),
            Err(e) => Err(e),
        }
    }

    pub fn pkg_services(&mut self) -> Result<Vec<PackageIdent>> {
        self.read_deps(MetaFile::Services)
    }

    pub fn resolved_services(&mut self) -> Result<Vec<PackageIdent>> {
        self.read_deps(MetaFile::ResolvedServices)
    }

    pub fn target(&mut self) -> Result<PackageTarget> {
        match self.read_metadata(MetaFile::Target) {
            Ok(None) => Err(Error::MetaFileNotFound(MetaFile::Target)),
            Ok(Some(data)) => PackageTarget::from_str(&data),
            Err(e) => Err(e),
        }
    }

    /// A plain string representation of the archive's file name.
    pub fn file_name(&self) -> String {
        self.path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    /// Given a package name and a path to a file as an `&str`, verify
    /// the files signature.
    ///
    /// # Failures
    ///
    /// * Fails if it cannot verify the signature for any reason
    pub fn verify<P: AsRef<Path>>(&self, cache_key_path: &P) -> Result<(String, String)> {
        artifact::verify(&self.path, cache_key_path)
    }

    /// Given a package name and a path to a file as an `&str`, unpack
    /// the package.
    ///
    /// # Failures
    ///
    /// * If the package cannot be unpacked
    pub fn unpack(&self, fs_root_path: Option<&Path>) -> Result<()> {
        let root = fs_root_path.unwrap_or_else(|| Path::new("/"));
        let tar_reader = artifact::get_archive_reader(&self.path)?;
        let mut builder = reader::Builder::new();
        builder.support_format(ReadFormat::Gnutar)?;
        builder.support_filter(ReadFilter::Xz)?;
        let mut reader = builder.open_stream(tar_reader)?;
        let writer = writer::Disk::new();
        let mut extract_options = ExtractOptions::new();
        extract_options.add(ExtractOption::Time);
        extract_options.add(ExtractOption::Permissions);
        writer.set_options(&extract_options)?;
        writer.set_standard_lookup()?;
        writer.write(&mut reader, Some(root.to_string_lossy().as_ref()))?;
        writer.close()?;
        Ok(())
    }

    fn read_deps(&mut self, file: MetaFile) -> Result<Vec<PackageIdent>> {
        let mut deps: Vec<PackageIdent> = vec![];

        // For now, all deps files but SERVICES need fully-qualified
        // package identifiers
        let must_be_fully_qualified = { file != MetaFile::Services };

        match self.read_metadata(file) {
            Ok(Some(body)) => {
                let ids: Vec<String> = body.lines().map(str::to_string).collect();
                for id in &ids {
                    let package = PackageIdent::from_str(id)?;
                    if !package.fully_qualified() && must_be_fully_qualified {
                        return Err(Error::FullyQualifiedPackageIdentRequired(package.to_string()));
                    }
                    deps.push(package);
                }
                Ok(deps)
            }
            Ok(None) => Ok(vec![]),
            Err(Error::MetaFileNotFound(_)) => Ok(vec![]),
            Err(e) => Err(e),
        }
    }

    fn read_metadata(&mut self, file: MetaFile) -> Result<Option<&String>> {
        if let Some(ref files) = self.metadata {
            return Ok(files.get(&file));
        }
        let mut metadata = Metadata::new();
        let mut matched_count = 0u8;
        let tar_reader = artifact::get_archive_reader(&self.path)?;
        let mut builder = reader::Builder::new();
        builder.support_format(ReadFormat::Gnutar)?;
        builder.support_filter(ReadFilter::Xz)?;
        let mut reader = builder.open_stream(tar_reader)?;
        loop {
            let mut matched_type: Option<MetaFile> = None;
            if let Some(entry) = reader.next_header() {
                for (matched, regx) in METAFILE_REGXS.iter() {
                    if regx.is_match(entry.pathname()) {
                        matched_type = Some(*matched);
                        matched_count += 1;
                        break;
                    }
                }
            } else {
                break;
            }

            if matched_type.is_none() {
                continue;
            }

            let mut buf = String::new();
            loop {
                match reader.read_block() {
                    Ok(Some(bytes)) => {
                        match str::from_utf8(bytes) {
                            Ok(content) => {
                                // You used to trim. Now you don't, because you were trimming
                                // in the wrong place. Sometimes a buffer ends (or starts!) with
                                // a newline.
                                buf.push_str(content);
                            }
                            Err(_) => return Err(Error::MetaFileMalformed(matched_type.unwrap())),
                        }
                    }
                    Ok(None) => {
                        // Hey, before you go - we are trimming whitespace for you. This
                        // is handy, because later on, you just want the string you want.
                        metadata.insert(matched_type.unwrap(), String::from(buf.trim()));
                        break;
                    }
                    Err(_) => return Err(Error::MetaFileMalformed(matched_type.unwrap())),
                }
            } // inner loop

            if matched_count == METAFILE_REGXS.len() as u8 {
                break;
            }
        }
        self.metadata = Some(metadata);
        Ok(self.metadata.as_ref().unwrap().get(&file))
    }
}

pub trait FromArchive: Sized {
    type Error: error::Error;

    fn from_archive(archive: &mut PackageArchive) -> result::Result<Self, Self::Error>;
}

#[cfg(test)]
mod test {
    use super::{super::target,
                *};
    use std::path::PathBuf;

    #[test]
    fn reading_artifact_metadata() {
        let mut hart = PackageArchive::new(fixtures().join("happyhumans-possums-8.1.\
                                                            4-20160427165340-x86_64-linux.hart"));
        let ident = hart.ident().unwrap();
        assert_eq!(ident.origin, "happyhumans");
        assert_eq!(ident.name, "possums");
        assert_eq!(ident.version, Some("8.1.4".to_string()));
        assert_eq!(ident.release, Some("20160427165340".to_string()));
    }

    pub fn root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests") }

    pub fn fixtures() -> PathBuf { root().join("fixtures") }

    #[test]
    fn reading_artifact_deps() {
        let mut hart = PackageArchive::new(fixtures().join("happyhumans-possums-8.1.\
                                                            4-20160427165340-x86_64-linux.hart"));
        let _ = hart.deps().unwrap();
        let _ = hart.tdeps().unwrap();
    }

    #[test]
    fn reading_artifact_large_tdeps() {
        let mut hart = PackageArchive::new(fixtures().join("unhappyhumans-possums-8.1.\
                                                            4-20160427165340-x86_64-linux.hart"));
        let tdeps = hart.tdeps().unwrap();
        assert_eq!(1024, tdeps.len());
    }

    #[test]
    fn reading_artifact_target() {
        let mut hart = PackageArchive::new(fixtures().join("unhappyhumans-possums-8.1.\
                                                            4-20160427165340-x86_64-linux.hart"));
        let target = hart.target().unwrap();

        assert_eq!(target::X86_64_LINUX, target);
    }
}
