from dependencies import *
from SWITCHlogger import logger


class SWITCHFileDownloader:

    _auth = None
    _src_dst_df = None
    _src_col = None
    _dst_col = None

    def __repr__(self) -> str:
        repr_str = (
            f"<{__class__.__name__}>\n"
            f'{self._auth.username=}\n'
            f'{self._auth.password=}\n'
            f'{self._src_dst_df=}\n'
            f'{self._src_col=}\n'
            f'{self._dst_col=}\n'
            f"self._src_dst_df=\n{self._src_dst_df}"
        )
        return repr_str


    @classmethod
    def set_login(cls, SWITCHemail:str, SWITCHpw:str) -> None:
        cls._auth = HTTPBasicAuth(
            SWITCHemail, SWITCHpw
                )


    @classmethod
    def set_SRC_DST_df(cls, src_dst_df:pd.DataFrame) -> None:
        cls._src_dst_df = src_dst_df


    @classmethod
    def set_src_dst_column_names(cls, src_col:str, dst_col:str) -> None:
        cls._src_col = src_col
        cls._dst_col = dst_col


    def _downloadFile(self, row:pd.Series) -> None:

        src_url = row[self._src_col]
        dst_pth = row[self._dst_col]

        try:
            logger.debug(f"... starting download from {src_url=}")

            r = requests.get(src_url, auth=self._auth, stream=True, verify=True)

            if r.ok:
                logger.debug(f"... saving to {dst_pth=}")
                with open(dst_pth, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 8):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                            os.fsync(f.fileno())
                logger.debug("... download completed successfully")
            else:  # HTTP status code 4XX/5XX
                logger.debug(f"... download failed: status code {r.status_code}\n{r.text}")

        except requests.RequestException as e:
            logger.debug(f"... request failed: {e}")

        except OSError as e:
            logger.debug(f"... file operation failed: {e}")


    def _checkup(self) -> None:
        attributes = {
            f'{self._auth.username=}': self._auth.username,
            f'{self._auth.password=}': self._auth.password,
            f'{self._src_dst_df=}': self._src_dst_df,
            f'{self._src_col=}': self._src_col,
            f'{self._dst_col=}': self._dst_col
        }
        
        missing_attrs = [attr for attr, value in attributes.items() if value is None]
        
        if missing_attrs:
            for attr in missing_attrs:
                logger.error(
                    f"... you must define the variable: {attr}"
                    )
            exit()

        def KeyInDf(attribute:str) -> None:
            if attributes[attribute] not in self._src_dst_df:
                logger.error(
                    f"... col name {attribute} does not exist. You must choose from "
                    f"{self._src_dst_df.columns=}"
                    )
                exit()
                
        KeyInDf(f'{self._src_col=}')
        KeyInDf(f'{self._dst_col=}')


    def go(self) -> None:
        self._checkup()
        logger.info(
            f"... downloading {len(self._src_dst_df)} files "
            "from '{self._src_col}' to '{self._dst_col}'"
            )
        df = self._src_dst_df.parallel_apply(self._downloadFile, axis=1)




if __name__ == '__main__':
    SWITCHFileDownloader.set_login(
        SWITCHemail='your_email',
        SWITCHpw='your_username'
        )
    
    print(SWITCHFileDownloader())

    myUpdater = SWITCHFileDownloader()
    myUpdater.go()