from dependencies import *
from SWITCHlogger import logger


class BFE_get_src_dst_df:

    _root = None
    _loc_file_col = None
    _df = None

    _DEFAULT_HDR_FREQUENCY = '15min'
    _DEFAULT_TEM_FREQUENCY = '1day'
    _DEFAULT_TIME_ZONE = 'Europe/Zurich'


    def __init__(self) -> None:
        super().__init__()


    @classmethod
    def _walkExistingFiles(cls, path:str) -> list:
        logger.info(f"... walking through directory '{path}'")
        all_files = []
        for root, dirs, files in os.walk(path):
            for file in files:
                all_files.append(os.path.join(root, file))
        logger.info(f" -> found {len(all_files)} files in '{path}'")
        return all_files
    
    @staticmethod
    def _extract_box_name_from_path(root:str) -> str:
        # walks through every element of the path and takes the one containg 'box'
        path_parts = os.path.normpath(root).split(os.sep) # [list, of, every, folder]
        return next((part for part in path_parts if 'box' in part), '')

    @classmethod
    def _dropNoGzFiles(cls, df:pd.DataFrame, file_col:str) -> pd.DataFrame:
        original_length = len(df)
        gz_filter = df[file_col].str.contains('.gz')
        df = df[gz_filter].reset_index(drop=True)
        logger.info(f"... dropped {original_length-len(df)} no .gz files")
        return df
    

    @classmethod
    def _extractFileBaseBoxname(cls, df:pd.DataFrame, file_col:str) -> pd.DataFrame:
        logger.info(f"... extract basenames from column '{file_col}'")
        df['basenames'] = df[file_col].apply(os.path.basename)
        logger.info(f"... extract box names from column '{file_col}'")
        df['boxes'] = df[file_col].apply(cls._extract_box_name_from_path)
        return df
    

    @classmethod
    def get_files(cls, root:str):
        cls._root = root
        loc_file_col = f"files in {root}"

        df = pd.DataFrame({loc_file_col:cls._walkExistingFiles(root)})

        if df.empty:
            logger.error(f"... directory for root {root} is empty")
            exit()

        df = cls._dropNoGzFiles(df, loc_file_col)
        cls._df = cls._extractFileBaseBoxname(df, loc_file_col)


    def _createDaterange(self, start:dt.datetime, end:dt.datetime, freq:str) -> pd.DataFrame:
        date_df = pd.DataFrame({
            'dates':pd.date_range(start=start, end=end, freq=freq, tz=self._DEFAULT_TIME_ZONE)
            })
        return date_df

    @staticmethod
    def _getHDRFilename(datetime: dt.datetime) -> str:
        return (
            f'image-{datetime.year}_{datetime.month:02d}_{datetime.day:02d}_'
            f'{datetime.hour:02d}_{datetime.minute:02d}.hdr.gz'
        )

    @staticmethod
    def _getTEMFilename(datetime:dt.datetime) -> str:
        return f'temperature-{datetime.year}{datetime.month:02d}{datetime.day:02d}.gz'


    @staticmethod
    def _getIRRFilename(datetime:dt.datetime) -> str:
        return f'irradiance-{datetime.year}{datetime.month:02d}{datetime.day:02d}.gz'


    def _guessFilenames(
            self,
            start:dt.datetime,
            end:dt.datetime,
            freq:str,
            guesser:Callable[[dt.datetime], str]
            ) -> pd.DataFrame:
        file_df = self._createDaterange(start, end, freq)
        file_df['guessed file names'] = file_df['dates'].apply(guesser)
        return file_df


    def _removeExistingFiles(
            self,
            existing_files:pd.DataFrame,
            guessed_files:pd.DataFrame,
            boxNr:int | None
            ) -> pd.DataFrame:

        if not boxNr:
            window = existing_files.loc[existing_files['boxes']=='']
            box_name = 'irr'
        else:
            box_name = f"box{boxNr}"
            window = existing_files.loc[existing_files['boxes']==box_name]

        guessed_files_filtered = guessed_files[
            ~guessed_files['guessed file names'].isin(window['basenames'])
            ]

        logger.info(
            f"... found {len(guessed_files)-len(guessed_files_filtered)} "
            f"existing files for '{box_name}' not to download"
            )

        return guessed_files_filtered.reset_index(drop=True)


    def _getDstPath(self, boxNr:int | None, filetype:str, file:str) -> str:
        if not boxNr:
            dst_path = os.path.join(self._root, 'irradiance', file)
        else:
            dst_path = os.path.join(self._root, f"box{boxNr}", filetype, file)
        return dst_path


    def _getSWITCHurl(self, boxNr:int) -> str:
        url = os.path.join(
            'https://drive.switch.ch/remote.php/webdav/Spool/', f'box{boxNr}', 'archive'
                )
        return url


    def _addDstPathSWTICHurl(self, df:pd.DataFrame, boxNr:int, filetype:str) -> pd.DataFrame:
        df['abs dst path'] = df['guessed file names'].apply(
            lambda row: self._getDstPath(boxNr, filetype, row)
            )
        root_url = self._getSWITCHurl(boxNr)
        df['SWITCH url'] = df['guessed file names'].apply(
            lambda row: os.path.join(root_url, row)
            )
        return df



    def to_df(
            self,
            boxNr:int,
            which:str|list[str],
            start:dt.datetime,
            end:dt.datetime=dt.datetime.today()
            ):
        
        if 'hdr' in which:
            hdr_file_df = self._guessFilenames(start, end, '15min', self._getHDRFilename)
            hdr_downloads = self._removeExistingFiles(self._df, hdr_file_df, boxNr)
            hdr_down = self._addDstPathSWTICHurl(hdr_downloads, boxNr, 'hdr')
            return hdr_down
        

        if 'tem' in which:
            tem_file_df = self._guessFilenames(start, end, 'D', self._getTEMFilename)
            tem_downloads = self._removeExistingFiles(self._df, tem_file_df, boxNr)
            tem_down = self._addDstPathSWTICHurl(tem_downloads, boxNr, 'tem')
            return tem_down

        if 'irr' in which:
            irr_file_df = self._guessFilenames(start, end, 'D', self._getIRRFilename)
            irr_downloads = self._removeExistingFiles(self._df, irr_file_df, None)
            irr_down = self._addDstPathSWTICHurl(irr_downloads, None, 'irr')
            return irr_down


if __name__ == '__main__':
    BFE_get_src_dst_df.get_files(root='raw')

    df_getter = BFE_get_src_dst_df()
    mydf = df_getter.to_df(boxNr=1, which='hdr', start=dt.datetime(2024, 9, 1))
    print(mydf)