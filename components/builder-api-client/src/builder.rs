use std::{fmt,
          fs::{self,
               File},
          io::{self,
               Read},
          path::{Path,
                 PathBuf},
          string::ToString};

use broadcast::BroadcastWriter;
use hyper::{client::{Body,
                     IntoUrl,
                     RequestBuilder,
                     Response},
            header::{Accept,
                     Authorization,
                     Bearer,
                     ContentType},
            status::StatusCode,
            Url};
use tee::TeeReader;
use url::percent_encoding::{percent_encode,
                            PATH_SEGMENT_ENCODE_SET};

use crate::{error::{Error,
                    Result},
            hab_core::{crypto::keys::box_key_pair::WrappedSealedBox,
                       fs::AtomicWriter,
                       package::{Identifiable,
                                 PackageArchive,
                                 PackageIdent,
                                 PackageTarget},
                       ChannelIdent},
            hab_http::{util::decoded_response,
                       ApiClient},
            BoxedClient,
            BuilderAPIProvider,
            DisplayProgress,
            OriginKeyIdent,
            OriginSecret,
            ReverseDependencies,
            SchedulerResponse};

header! { (XFileName, "X-Filename") => [String] }
header! { (ETag, "ETag") => [String] }

const DEFAULT_API_PATH: &str = "/v1";

#[derive(Clone, Deserialize)]
#[serde(rename = "error")]
pub struct NetError {
    pub code: i32,
    pub msg:  String,
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[err: {:?}, msg: {}]", self.code, self.msg)
    }
}

/// Custom conversion logic to allow `serde` to successfully
/// round-trip `u64` datatypes through JSON serialization.
///
/// To use it, add `#[serde(with = "json_u64")]` to any `u64`-typed struct
/// fields.
mod json_u64 {
    use serde::{self,
                Deserialize,
                Deserializer,
                Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(num: &u64, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let s = format!("{}", num);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<u64>().map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OriginPrivateSigningKey {
    #[serde(with = "json_u64")]
    pub id: u64,
    #[serde(with = "json_u64")]
    pub origin_id: u64,
    pub name: String,
    pub revision: String,
    pub body: Vec<u8>,
    #[serde(with = "json_u64")]
    pub owner_id: u64,
}

mod json {
    #[derive(Clone, Deserialize)]
    pub struct Package {
        pub ident:    PackageIdent,
        pub checksum: String,
        pub manifest: String,
        pub deps:     Vec<PackageIdent>,
        pub tdeps:    Vec<PackageIdent>,
        pub exposes:  Vec<u32>,
        pub config:   String,
    }

    #[derive(Clone, Deserialize)]
    pub struct PackageIdent {
        pub origin:  String,
        pub name:    String,
        pub version: String,
        pub release: String,
    }

    impl From<PackageIdent> for super::PackageIdent {
        fn from(ident: PackageIdent) -> Self {
            super::PackageIdent { origin:  ident.origin,
                                  name:    ident.name,
                                  version: Some(ident.version),
                                  release: Some(ident.release), }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PackageResults<T> {
    pub range_start: isize,
    pub range_end:   isize,
    pub total_count: isize,
    pub data:        Vec<T>,
}

#[derive(Clone, Deserialize)]
pub struct OriginChannelIdent {
    pub name: String,
}

pub struct BuilderAPIClient(ApiClient);

impl BuilderAPIClient {
    pub fn new<U>(endpoint: U,
                  product: &str,
                  version: &str,
                  fs_root_path: Option<&Path>)
                  -> Result<Self>
        where U: IntoUrl
    {
        let mut endpoint = endpoint.into_url().map_err(Error::UrlParseError)?;
        if !endpoint.cannot_be_a_base() && endpoint.path() == "/" {
            endpoint.set_path(DEFAULT_API_PATH);
        }
        let client = BuilderAPIClient(
            ApiClient::new(endpoint, product, version, fs_root_path)
                .map_err(Error::HabitatHttpClient)?,
        );
        Ok(client)
    }

    pub fn create<U>(endpoint: U,
                     product: &str,
                     version: &str,
                     fs_root_path: Option<&Path>)
                     -> Result<BoxedClient>
        where U: IntoUrl
    {
        Self::new(endpoint, product, version, fs_root_path).map(|c| Box::new(c) as _)
    }

    fn maybe_add_authz<'a>(&'a self,
                           rb: RequestBuilder<'a>,
                           token: Option<&str>)
                           -> RequestBuilder<'_> {
        if token.is_some() {
            rb.header(Authorization(Bearer { token: token.unwrap().to_string(), }))
        } else {
            rb
        }
    }

    fn add_authz<'a>(&'a self, rb: RequestBuilder<'a>, token: &str) -> RequestBuilder<'_> {
        rb.header(Authorization(Bearer { token: token.to_string(), }))
    }

    fn download<'a>(&'a self,
                    rb: RequestBuilder<'a>,
                    dst_path: &Path,
                    token: Option<&str>,
                    progress: Option<<BuilderAPIClient as BuilderAPIProvider>::Progress>)
                    -> Result<PathBuf> {
        let mut res = self.maybe_add_authz(rb, token).send()?;

        debug!("Response: {:?}", res);

        if res.status != hyper::status::StatusCode::Ok {
            return Err(err_from_response(res));
        }

        fs::create_dir_all(&dst_path)?;

        let file_name = res.headers
                           .get::<XFileName>()
                           .expect("XFileName missing from response")
                           .to_string();
        let dst_file_path = dst_path.join(file_name);
        let w = AtomicWriter::new(&dst_file_path)?;
        w.with_writer(|mut f| {
             match progress {
                 Some(mut progress) => {
                     let size: u64 = res.headers
                                        .get::<hyper::header::ContentLength>()
                                        .map_or(0, |v| **v);
                     progress.size(size);
                     let mut writer = BroadcastWriter::new(&mut f, progress);
                     io::copy(&mut res, &mut writer)
                 }
                 None => io::copy(&mut res, &mut f),
             }
         })
         .map_err(Error::BadResponseBody)?;
        Ok(dst_file_path)
    }

    fn seach_package_with_range(&self,
                                search_term: &str,
                                token: Option<&str>,
                                range: usize)
                                -> Result<(PackageResults<PackageIdent>, bool)> {
        let req = self.0
                      .get_with_custom_url(&package_search(search_term), |url| {
                          url.set_query(Some(&format!("range={:?}&distinct=true", range)));
                      });
        let mut res = self.maybe_add_authz(req, token).send()?;
        let mut encoded = String::new();
        res.read_to_string(&mut encoded)
           .map_err(Error::BadResponseBody)?;
        let package_results = serde_json::from_str(&encoded)?;
        match res.status {
            StatusCode::Ok => Ok((package_results, false)),
            StatusCode::PartialContent => Ok((package_results, true)),
            _ => Err(err_from_response(res)),
        }
    }

    fn search_package_impl(&self,
                           search_term: &str,
                           limit: usize,
                           token: Option<&str>,
                           search_with_range: impl Fn(&BuilderAPIClient,
                              &str,
                              Option<&str>,
                              usize)
                              -> Result<(PackageResults<PackageIdent>, bool)>)
                           -> Result<(Vec<PackageIdent>, usize)> {
        let mut packages = Vec::new();
        loop {
            let (mut package_results, more_to_come) =
                search_with_range(self, search_term, token, packages.len())?;
            packages.append(&mut package_results.data);

            if packages.len() >= limit || !more_to_come {
                packages.truncate(limit);
                return Ok((packages, package_results.total_count as usize));
            }
        }
    }
}

impl BuilderAPIProvider for BuilderAPIClient {
    type Progress = Box<dyn DisplayProgress>;

    /// Retrieves the status of every group job in an origin
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn get_origin_schedule(&self, origin: &str, limit: usize) -> Result<Vec<SchedulerResponse>> {
        debug!("Retrieving status for job groups in the {} origin", origin);

        let path = format!("depot/pkgs/schedule/{}/status", origin);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("limit", &limit.to_string());
        };

        let res = self.0.get_with_custom_url(&path, custom).send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let sr: Vec<SchedulerResponse> = decoded_response(res)?;
        Ok(sr)
    }

    /// Retrieves the status of a group job
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn get_schedule(&self, group_id: i64, include_projects: bool) -> Result<SchedulerResponse> {
        debug!("Retrieving schedule for job group {}", group_id);

        let path = format!("depot/pkgs/schedule/{}", group_id);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("include_projects", &include_projects.to_string());
        };

        let res = self.0.get_with_custom_url(&path, custom).send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let sr: SchedulerResponse = decoded_response(res)?;
        Ok(sr)
    }

    /// Schedules a job for a package ident
    ///
    /// # Failures
    ///
    /// * Key cannot be found
    /// * Remote Builder is not available
    fn schedule_job(&self,
                    (ident, target): (&PackageIdent, PackageTarget),
                    package_only: bool,
                    token: &str)
                    -> Result<(String)> {
        let path = format!("depot/pkgs/schedule/{}/{}", ident.origin(), ident.name());

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("package_only", &package_only.to_string())
               .append_pair("target", &target.to_string());
        };

        match self.add_authz(self.0.post_with_custom_url(&path, custom), token)
                  .send()
        {
            Ok(response) => {
                if response.status == StatusCode::Created || response.status == StatusCode::Ok {
                    let sr: SchedulerResponse = decoded_response(response)?;
                    Ok(sr.id)
                } else {
                    Err(err_from_response(response))
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Fetch the reverse dependencies for a package
    ///
    /// # Failures
    ///
    /// * Remote API Server is not available
    fn fetch_rdeps(&self, (ident, target): (&PackageIdent, PackageTarget)) -> Result<Vec<String>> {
        debug!("Fetching the reverse dependencies for {}", ident);

        let url = format!("rdeps/{}", ident);

        let mut res = self.0
                          .get_with_custom_url(&url, |u| {
                              u.set_query(Some(&format!("target={}", &target.to_string())))
                          })
                          .send()
                          .map_err(Error::HyperError)?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let mut encoded = String::new();
        res.read_to_string(&mut encoded).map_err(Error::IO)?;
        debug!("Body: {:?}", encoded);
        let rd: ReverseDependencies = serde_json::from_str(&encoded).map_err(Error::Json)?;
        Ok(rd.rdeps.to_vec())
    }

    /// Promote/Demote a job group to/from a channel
    ///
    /// # Failures
    ///
    /// * Remote API Server is not available
    fn job_group_promote_or_demote(&self,
                                   group_id: u64,
                                   idents: &[String],
                                   channel: &ChannelIdent,
                                   token: &str,
                                   promote: bool)
                                   -> Result<()> {
        let json_idents = json!(idents);
        let body = json!({ "idents": json_idents });
        let sbody = serde_json::to_string(&body).unwrap();
        let url = format!("jobs/group/{}/{}/{}",
                          group_id,
                          if promote { "promote" } else { "demote" },
                          channel);
        let res = self.add_authz(self.0.post(&url), token)
                      .body(&sbody)
                      .header(Accept::json())
                      .header(ContentType::json())
                      .send()
                      .map_err(Error::HyperError)?;

        if res.status != StatusCode::NoContent {
            debug!("Failed to {} group, status: {:?}",
                   if promote { "promote" } else { "demote" },
                   res.status);
            return Err(err_from_response(res));
        }

        Ok(())
    }

    /// Cancel a job group
    ///
    /// # Failures
    ///
    /// * Remote API Server is not available
    fn job_group_cancel(&self, group_id: u64, token: &str) -> Result<()> {
        let url = format!("jobs/group/{}/cancel", group_id);
        let res = self.add_authz(self.0.post(&url), token)
                      .send()
                      .map_err(Error::HyperError)?;

        if res.status != StatusCode::NoContent {
            debug!("Failed to cancel group, status: {:?}", res.status);
            return Err(err_from_response(res));
        }

        Ok(())
    }

    /// Download a public encryption key from a remote Builder to the given filepath.
    ///
    /// # Failures
    ///
    /// * Key cannot be found
    /// * Remote Builder is not available
    /// * File cannot be created and written to
    fn fetch_origin_public_encryption_key(&self,
                                          origin: &str,
                                          token: &str,
                                          dst_path: &Path,
                                          progress: Option<Self::Progress>)
                                          -> Result<PathBuf> {
        self.download(self.0
                          .get(&format!("depot/origins/{}/encryption_key", origin)),
                      dst_path.as_ref(),
                      Some(token),
                      progress)
    }

    /// Create an origin
    ///
    ///  # Failures
    ///
    ///  * Remote builder is not available
    ///  * Unable to authenticate
    fn create_origin(&self, origin: &str, token: &str) -> Result<()> {
        let body = json!({
            "name": origin,
        });

        let sbody = serde_json::to_string(&body)?;

        let res = self.add_authz(self.0.post("depot/origins"), token)
                      .body(&sbody)
                      .header(Accept::json())
                      .header(ContentType::json())
                      .send()?;

        if res.status != StatusCode::Created {
            return Err(err_from_response(res));
        }

        Ok(())
    }

    /// Create secret for an origin
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn create_origin_secret(&self,
                            origin: &str,
                            token: &str,
                            key: &str,
                            secret: &WrappedSealedBox)
                            -> Result<()> {
        let path = format!("depot/origins/{}/secret", origin);
        let body = json!({
            "name": key,
            "value": secret
        });

        let sbody = serde_json::to_string(&body)?;
        let res = self.add_authz(self.0.post(&path), token)
                      .body(&sbody)
                      .header(Accept::json())
                      .header(ContentType::json())
                      .send()?;

        if res.status != StatusCode::Created {
            return Err(err_from_response(res));
        }

        Ok(())
    }

    /// Delete a secret for an origin
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn delete_origin_secret(&self, origin: &str, token: &str, key: &str) -> Result<()> {
        let path = format!("depot/origins/{}/secret/{}", origin, key);

        let res = self.add_authz(self.0.delete(&path), token).send()?;

        // We expect NoContent, because the origin must be empty to delete
        if res.status == StatusCode::NoContent {
            Ok(())
        } else {
            Err(err_from_response(res))
        }
    }

    /// Delete an origin
    ///
    ///  # Failures
    ///
    ///  * Remote builder is not available
    ///  * Origin is populated with > 0 packages
    ///  * Submitted Origin is not found
    fn delete_origin(&self, origin: &str, token: &str) -> Result<()> {
        let path = format!("depot/origins/{}", origin);

        let res = self.add_authz(self.0.delete(&path), token).send()?;

        if res.status != StatusCode::NoContent {
            return Err(err_from_response(res));
        }

        Ok(())
    }

    /// List all secrets keys for an origin
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn list_origin_secrets(&self, origin: &str, token: &str) -> Result<Vec<String>> {
        let path = format!("depot/origins/{}/secret", origin);

        let mut res = self.add_authz(self.0.get(&path), token).send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let mut encoded = String::new();
        res.read_to_string(&mut encoded)
           .map_err(Error::BadResponseBody)?;
        debug!("Response body: {:?}", encoded);
        let secret_keys: Vec<String> =
            serde_json::from_str::<Vec<OriginSecret>>(&encoded)?.into_iter()
                                                                .map(|s| s.name)
                                                                .collect();
        Ok(secret_keys)
    }

    /// Download a public key from a remote Builder to the given filepath.
    ///
    /// # Failures
    ///
    /// * Key cannot be found
    /// * Remote Builder is not available
    /// * File cannot be created and written to
    fn fetch_origin_key(&self,
                        origin: &str,
                        revision: &str,
                        _token: Option<&str>,
                        dst_path: &Path,
                        progress: Option<Self::Progress>)
                        -> Result<PathBuf> {
        self.download(self.0
                          .get(&format!("depot/origins/{}/keys/{}", origin, revision)),
                      dst_path.as_ref(),
                      None,
                      progress)
    }

    /// Download a secret key from a remote Builder to the given filepath.
    ///
    /// # Failures
    ///
    /// * Key cannot be found
    /// * Remote Builder is not available
    /// * File cannot be created and written to
    fn fetch_secret_origin_key(&self,
                               origin: &str,
                               token: &str,
                               dst_path: &Path,
                               progress: Option<Self::Progress>)
                               -> Result<PathBuf> {
        self.download(self.0
                          .get(&format!("depot/origins/{}/secret_keys/latest", origin)),
                      dst_path.as_ref(),
                      Some(token),
                      progress)
    }

    fn show_origin_keys(&self, origin: &str) -> Result<Vec<OriginKeyIdent>> {
        let mut res = self.0.get(&origin_keys_path(origin)).send()?;
        debug!("Response: {:?}", res);

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        };

        let mut encoded = String::new();
        res.read_to_string(&mut encoded)
           .map_err(Error::BadResponseBody)?;
        debug!("Response body: {:?}", encoded);
        let revisions: Vec<OriginKeyIdent> = serde_json::from_str::<Vec<OriginKeyIdent>>(&encoded)?;
        Ok(revisions)
    }

    /// Return a list of channels for a given package
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    /// * Package does not exist
    fn package_channels(&self,
                        (ident, target): (&PackageIdent, PackageTarget),
                        token: Option<&str>)
                        -> Result<Vec<String>> {
        if !ident.fully_qualified() {
            return Err(Error::IdentNotFullyQualified);
        }

        let path = package_channels_path(ident);
        debug!("Retrieving channels for {}, target {}", ident, target);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("target", &target.to_string());
        };

        let mut res = self.maybe_add_authz(self.0.get_with_custom_url(&path, custom), token)
                          .send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let mut encoded = String::new();
        res.read_to_string(&mut encoded)
           .map_err(Error::BadResponseBody)?;
        debug!("Response body: {:?}", encoded);
        let channels: Vec<String> = serde_json::from_str::<Vec<String>>(&encoded)?.into_iter()
                                                                                  .collect();
        Ok(channels)
    }

    /// Upload a public origin key to a remote Builder.
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    /// * File cannot be read
    ///
    /// # Panics
    ///
    /// * Authorization token was not set on client
    fn put_origin_key(&self,
                      origin: &str,
                      revision: &str,
                      src_path: &Path,
                      token: &str,
                      progress: Option<Self::Progress>)
                      -> Result<()> {
        let path = format!("depot/origins/{}/keys/{}", &origin, &revision);
        let mut file =
            File::open(src_path).map_err(|e| Error::KeyReadError(src_path.to_path_buf(), e))?;
        let file_size = file.metadata()
                            .map_err(|e| Error::KeyReadError(src_path.to_path_buf(), e))?
                            .len();

        let result = if let Some(mut progress) = progress {
            progress.size(file_size);
            let mut reader = TeeReader::new(file, progress);
            self.add_authz(self.0.post(&path), token)
                .body(Body::SizedBody(&mut reader, file_size))
                .send()
        } else {
            self.add_authz(self.0.post(&path), token)
                .body(Body::SizedBody(&mut file, file_size))
                .send()
        };
        match result {
            Ok(Response { status: StatusCode::Created,
                          .. }) => Ok(()),
            Ok(response) => Err(err_from_response(response)),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Upload a secret origin key to a remote Builder.
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    /// * File cannot be read
    ///
    /// # Panics
    ///
    /// * Authorization token was not set on client
    fn put_origin_secret_key(&self,
                             origin: &str,
                             revision: &str,
                             src_path: &Path,
                             token: &str,
                             progress: Option<Self::Progress>)
                             -> Result<()> {
        let path = format!("depot/origins/{}/secret_keys/{}", &origin, &revision);
        let mut file =
            File::open(src_path).map_err(|e| Error::KeyReadError(src_path.to_path_buf(), e))?;
        let file_size = file.metadata()
                            .map_err(|e| Error::KeyReadError(src_path.to_path_buf(), e))?
                            .len();

        let result = if let Some(mut progress) = progress {
            progress.size(file_size);
            let mut reader = TeeReader::new(file, progress);
            self.add_authz(self.0.post(&path), token)
                .body(Body::SizedBody(&mut reader, file_size))
                .send()
        } else {
            self.add_authz(self.0.post(&path), token)
                .body(Body::SizedBody(&mut file, file_size))
                .send()
        };
        match result {
            Ok(Response { status: StatusCode::Created,
                          .. }) => Ok(()),
            Ok(response) => Err(err_from_response(response)),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Download the latest release of a package.
    ///
    /// By the time this function is called, the ident must be fully qualified. The download URL in
    /// the depot requires a fully qualified ident to work. If you want the latest version of
    /// a package, e.g. core/redis, you can display package details for that via a different URL,
    /// e.g. /pkgs/core/redis/latest but that only _shows_ you the details - it doesn't download
    /// the package.
    ///
    /// # Failures
    ///
    /// * Package cannot be found
    /// * Remote Builder is not available
    /// * File cannot be created and written to
    fn fetch_package(&self,
                     (ident, target): (&PackageIdent, PackageTarget),
                     token: Option<&str>,
                     dst_path: &Path,
                     progress: Option<Self::Progress>)
                     -> Result<PackageArchive> {
        // Ensure ident is fully qualified.
        //
        // TODO fn: this will be removed when we can describe a fully qualified ident by type as a
        // param to this function
        if !ident.fully_qualified() {
            return Err(Error::IdentNotFullyQualified);
        }

        let req_builder = self.0.get_with_custom_url(&package_download(ident), |u| {
                                    u.set_query(Some(&format!("target={}", target)))
                                });

        match self.download(req_builder, dst_path.as_ref(), token, progress) {
            Ok(file) => Ok(PackageArchive::new(file)),
            Err(e) => Err(e),
        }
    }

    /// Checks whether a specified package exists
    ///
    /// The package ident must be fully qualified
    ///
    /// # Failures
    ///
    /// * Package cannot be found
    /// * Remote Builder is not available
    fn check_package(&self,
                     (package, target): (&PackageIdent, PackageTarget),
                     token: Option<&str>)
                     -> Result<()> {
        if !package.fully_qualified() {
            return Err(Error::IdentNotFullyQualified);
        }

        let url = channel_package_path(&ChannelIdent::unstable(), package);

        let res = self.maybe_add_authz(self.0.get_with_custom_url(&url, |u| {
                                                 u.set_query(Some(&format!("target={}", target)))
                                             }),
                                       token)
                      .send()?;

        if res.status != StatusCode::Ok {
            Err(err_from_response(res))
        } else {
            Ok(())
        }
    }

    /// Returns a package struct for the latest package.
    ///
    /// An optional version can be specified which will scope the release returned to the latest
    /// release of that package.
    ///
    /// # Failures
    ///
    /// * Package cannot be found
    /// * Remote Builder is not available
    fn show_package(&self,
                    (package, target): (&PackageIdent, PackageTarget),
                    channel: &ChannelIdent,
                    token: Option<&str>)
                    -> Result<PackageIdent> {
        let mut url = channel_package_path(channel, package);

        if !package.fully_qualified() {
            url.push_str("/latest");
        }

        let mut res = self.maybe_add_authz(self.0
                                               .get_with_custom_url(&url, |u| {
                                                   u.set_query(Some(&format!("target={}", target)))
                                               }),
                                           token)
                          .send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        }

        let mut encoded = String::new();
        res.read_to_string(&mut encoded)
           .map_err(Error::BadResponseBody)?;
        debug!("Body: {:?}", encoded);
        let package: json::Package = serde_json::from_str::<json::Package>(&encoded)?;
        Ok(package.ident.into())
    }

    /// Upload a package to a remote Builder.
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    /// * File cannot be read
    ///
    /// # Panics
    ///
    /// * Authorization token was not set on client
    fn put_package(&self,
                   pa: &mut PackageArchive,
                   token: &str,
                   force_upload: bool,
                   progress: Option<Self::Progress>)
                   -> Result<()> {
        let checksum = pa.checksum()?;
        let ident = pa.ident()?;
        let target = pa.target()?;

        let file = File::open(&pa.path).map_err(|e| Error::PackageReadError(pa.path.clone(), e))?;
        let file_size = file.metadata()
                            .map_err(|e| Error::PackageReadError(pa.path.clone(), e))?
                            .len();

        let path = package_path(&ident);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("checksum", &checksum)
               .append_pair("target", &target.to_string())
               .append_pair("forced", &force_upload.to_string());
        };
        debug!("Reading from {}", &pa.path.display());

        let mut reader: Box<dyn Read> = if let Some(mut progress) = progress {
            progress.size(file_size);
            Box::new(TeeReader::new(file, progress))
        } else {
            Box::new(file)
        };

        let result = self.add_authz(self.0.post_with_custom_url(&path, custom), token)
                         .body(Body::SizedBody(&mut reader, file_size))
                         .send();

        match result {
            Ok(Response { status: StatusCode::Created,
                          .. }) => Ok(()),
            Ok(response) => Err(err_from_response(response)),
            Err(e) => Err(Error::from(e)),
        }
    }

    fn x_put_package(&self, pa: &mut PackageArchive, token: &str) -> Result<()> {
        let checksum = pa.checksum()?;
        let ident = pa.ident()?;
        let target = pa.target()?;

        let mut file =
            File::open(&pa.path).map_err(|e| Error::PackageReadError(pa.path.clone(), e))?;
        let file_size = file.metadata()
                            .map_err(|e| Error::PackageReadError(pa.path.clone(), e))?
                            .len();
        let path = package_path(&ident);
        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("checksum", &checksum)
               .append_pair("target", &target.to_string())
               .append_pair("builder", "");
        };
        debug!("Reading from {}", &pa.path.display());

        let result = self.add_authz(self.0.post_with_custom_url(&path, custom), token)
                         .body(Body::SizedBody(&mut file, file_size))
                         .send();
        match result {
            Ok(Response { status: StatusCode::Created,
                          .. }) => Ok(()),
            Ok(response) => Err(err_from_response(response)),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Delete a package from Builder
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    /// * If package does not exist in Builder
    /// * If the package does not qualify for deletion
    /// * Authorization token was not set on client
    fn delete_package(&self,
                      (ident, target): (&PackageIdent, PackageTarget),
                      token: &str)
                      -> Result<()> {
        let path = package_path(ident);
        debug!("Deleting package {}, target {}", ident, target);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("target", &target.to_string());
        };

        let res = self.add_authz(self.0.delete_with_custom_url(&path, custom), token)
                      .send()?;

        if res.status != StatusCode::NoContent {
            return Err(err_from_response(res));
        };

        Ok(())
    }

    /// Promote a package to a given channel
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    ///
    /// # Panics
    ///
    /// * If package does not exist in Builder
    /// * Authorization token was not set on client
    fn promote_package(&self,
                       (ident, target): (&PackageIdent, PackageTarget),
                       channel: &ChannelIdent,
                       token: &str)
                       -> Result<()> {
        if !ident.fully_qualified() {
            return Err(Error::IdentNotFullyQualified);
        }
        let path = channel_package_promote(channel, ident);
        debug!("Promoting package {}, target {}", ident, target);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("target", &target.to_string());
        };

        let res = self.add_authz(self.0.put_with_custom_url(&path, custom), token)
                      .send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        };

        Ok(())
    }

    /// Demote a package from a given channel
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    ///
    /// # Panics
    ///
    /// * If package does not exist in Builder
    /// * Authorization token was not set on client
    fn demote_package(&self,
                      (ident, target): (&PackageIdent, PackageTarget),
                      channel: &ChannelIdent,
                      token: &str)
                      -> Result<()> {
        if !ident.fully_qualified() {
            return Err(Error::IdentNotFullyQualified);
        }
        let path = channel_package_demote(channel, ident);
        debug!("Demoting package {}, target {}", ident, target);

        let custom = |url: &mut Url| {
            url.query_pairs_mut()
               .append_pair("target", &target.to_string());
        };

        let res = self.add_authz(self.0.put_with_custom_url(&path, custom), token)
                      .send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        };

        Ok(())
    }

    /// Create a custom channel
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn create_channel(&self, origin: &str, channel: &ChannelIdent, token: &str) -> Result<()> {
        let path = format!("depot/channels/{}/{}", origin, channel);
        debug!("Creating channel, path: {:?}", path);

        let res = self.add_authz(self.0.post(&path), token).send()?;

        if res.status != StatusCode::Created {
            return Err(err_from_response(res));
        };

        Ok(())
    }

    /// Delete a custom channel
    ///
    /// # Failures
    ///
    /// * Remote Builder is not available
    fn delete_channel(&self, origin: &str, channel: &ChannelIdent, token: &str) -> Result<()> {
        let path = format!("depot/channels/{}/{}", origin, channel);
        debug!("Deleting channel, path: {:?}", path);

        let res = self.add_authz(self.0.delete(&path), token).send()?;

        if res.status != StatusCode::Ok {
            return Err(err_from_response(res));
        };

        Ok(())
    }

    /// Returns a vector of PackageIdent structs
    ///
    /// # Failures
    ///
    /// * Remote depot unavailable
    fn search_package(&self,
                      search_term: &str,
                      limit: usize,
                      token: Option<&str>)
                      -> Result<(Vec<PackageIdent>, usize)> {
        self.search_package_impl(search_term, limit, token, Self::seach_package_with_range)
    }

    /// Return a list of channels for a given origin
    ///
    /// # Failures
    /// * Remote Builder is not available
    /// * Authorization token was not set on client
    fn list_channels(&self, origin: &str, include_sandbox_channels: bool) -> Result<Vec<String>> {
        let path = format!("depot/channels/{}", origin);
        let mut res = if include_sandbox_channels {
            self.0
                .get_with_custom_url(&path, |url| url.set_query(Some("sandbox=true")))
                .send()?
        } else {
            self.0.get(&path).send()?
        };

        match res.status {
            StatusCode::Ok | StatusCode::PartialContent => {
                let mut encoded = String::new();
                res.read_to_string(&mut encoded)
                   .map_err(Error::BadResponseBody)?;
                let results: Vec<OriginChannelIdent> = serde_json::from_str(&encoded)?;
                let channels = results.into_iter().map(|o| o.name).collect();
                Ok(channels)
            }
            _ => Err(err_from_response(res)),
        }
    }
}

fn err_from_response(mut response: hyper::client::Response) -> Error {
    if response.status == StatusCode::Unauthorized {
        return Error::APIError(response.status,
                               "Please check that you have specified a valid Personal Access \
                                Token."
                                       .to_string());
    }

    let mut buff = String::new();
    match response.read_to_string(&mut buff) {
        Ok(_) => {
            match serde_json::from_str::<NetError>(&buff) {
                Ok(err) => Error::APIError(response.status, err.to_string()),
                Err(_) => Error::APIError(response.status, buff),
            }
        }
        Err(_) => {
            buff.truncate(0);
            Error::APIError(response.status, buff)
        }
    }
}

fn origin_keys_path(origin: &str) -> String { format!("depot/origins/{}/keys", origin) }

fn package_download(package: &PackageIdent) -> String {
    format!("{}/download", package_path(package))
}

fn package_path(package: &PackageIdent) -> String { format!("depot/pkgs/{}", package) }

fn package_search(term: &str) -> String {
    let encoded_term = percent_encode(term.as_bytes(), PATH_SEGMENT_ENCODE_SET);
    format!("depot/pkgs/search/{}", encoded_term)
}

fn channel_package_path(channel: &ChannelIdent, package: &PackageIdent) -> String {
    let mut path = format!("depot/channels/{}/{}/pkgs/{}",
                           package.origin(),
                           channel,
                           package.name());
    if let Some(version) = package.version() {
        path.push_str("/");
        path.push_str(version);
        if let Some(release) = package.release() {
            path.push_str("/");
            path.push_str(release);
        }
    }
    path
}

fn package_channels_path(package: &PackageIdent) -> String {
    format!("depot/pkgs/{}/{}/{}/{}/channels",
            package.origin(),
            package.name(),
            package.version().unwrap(),
            package.release().unwrap())
}

fn channel_package_promote(channel: &ChannelIdent, package: &PackageIdent) -> String {
    format!("depot/channels/{}/{}/pkgs/{}/{}/{}/promote",
            package.origin(),
            channel,
            package.name(),
            package.version().unwrap(),
            package.release().unwrap())
}

fn channel_package_demote(channel: &ChannelIdent, package: &PackageIdent) -> String {
    format!("depot/channels/{}/{}/pkgs/{}/{}/{}/demote",
            package.origin(),
            channel,
            package.name(),
            package.version().unwrap(),
            package.release().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn json_round_trip_u64_fields() {
        let pre = OriginPrivateSigningKey { id:        705_705_315_793_903_646,
                                            origin_id: 705_705_305_031_319_582,
                                            name:      "core".to_string(),
                                            revision:  "20160810182414".to_string(),
                                            body:      vec![1, 2, 3],
                                            owner_id:  0, };

        let as_json = serde_json::to_value(&pre).unwrap();
        let expected = json!({
            "id": "705705315793903646",
            "origin_id": "705705305031319582",
            "name": "core",
            "revision": "20160810182414",
            "body": [
                1,
                2,
                3
            ],
            "owner_id": "0"
        });
        assert_eq!(as_json, expected);

        let post: OriginPrivateSigningKey = serde_json::from_value(as_json).unwrap();
        assert_eq!(pre.id, post.id);
        assert_eq!(pre.origin_id, post.origin_id);
        assert_eq!(pre.owner_id, post.owner_id);
    }

    fn get_test_ident(name: &str) -> PackageIdent {
        PackageIdent { origin:  String::from("test"),
                       name:    String::from(name),
                       version: None,
                       release: None, }
    }

    fn seach_generator<'a>(
        data: &'a [&str],
        step: usize)
        -> impl Fn(&BuilderAPIClient,
                  &str,
                  Option<&str>,
                  usize) -> Result<(PackageResults<PackageIdent>, bool)>
               + 'a {
        move |_client, search_term, _token, range| {
            let filtered = data.iter()
                               .filter(|d| d.contains(search_term))
                               .collect::<Vec<_>>();

            if filtered.is_empty() {
                return Ok((PackageResults { range_start: 0,
                                            range_end:   0,
                                            total_count: 0,
                                            data:        vec![], },
                           false));
            }

            let total = filtered.len();
            let last = total - 1;
            let (start, end) = if range >= last {
                (last, last)
            } else {
                (range, (range + step).min(last))
            };
            let filtered_range = filtered[start..=end].iter()
                                                      .map(|s| get_test_ident(**s))
                                                      .collect::<Vec<_>>();
            let result = PackageResults { range_start: start as isize,
                                          range_end:   end as isize,
                                          total_count: total as isize,
                                          data:        filtered_range, };
            Ok((result, end < last))
        }
    }

    #[test]
    fn package_search() {
        let client = BuilderAPIClient::new("http://test.com", "", "", None).expect("valid client");

        let sample_data = vec!["one_a", "one_b", "one_c", "one_d", "one_e", "two_a", "two_b",
                               "two_c", "two_d", "two_e"];

        let searcher = seach_generator(sample_data.as_slice(), 2);
        let r = client.search_package_impl("one", 10, None, searcher)
                      .expect("valid search");
        assert_eq!(r.0.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
                   vec!["one_a", "one_b", "one_c", "one_d", "one_e"]);
        assert_eq!(r.1, 5);

        let searcher = seach_generator(sample_data.as_slice(), 2);
        let r = client.search_package_impl("_", 3, None, searcher)
                      .expect("valid search");
        assert_eq!(r.0.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
                   vec!["one_a", "one_b", "one_c"]);
        assert_eq!(r.1, 10);

        let searcher = seach_generator(sample_data.as_slice(), 10);
        let r = client.search_package_impl("a", 2, None, searcher)
                      .expect("valid search");
        assert_eq!(r.0.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
                   vec!["one_a", "two_a"]);
        assert_eq!(r.1, 2);

        let searcher = seach_generator(sample_data.as_slice(), 10);
        let r = client.search_package_impl("does_not_exist", 100, None, searcher)
                      .expect("valid search");
        assert_eq!(r.0.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
                   Vec::<String>::new());
        assert_eq!(r.1, 0);
    }

    #[ignore]
    #[test]
    fn package_search_large() {
        let client = BuilderAPIClient::new("http://test.com", "", "", None).expect("valid client");

        let count = 100_000;
        let sample_data = std::iter::repeat("test").take(count).collect::<Vec<_>>();

        let searcher = seach_generator(sample_data.as_slice(), 50);
        let r = client.search_package_impl("test", count, None, searcher)
                      .expect("valid search");
        assert_eq!(r.1, count);
    }
}
