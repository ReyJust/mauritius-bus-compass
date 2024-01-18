from typing import Dict, Any
from kedro.io import AbstractDataset, DatasetError
from typing import List, Literal

import camelot
import pandas as pd
import PyPDF2

class PDFPageDataset(AbstractDataset):
    """
    Dataset that only get the tables of a PDF.
    Return the tables of each pages in an array
    """
    def __init__(
        self,
        filepath: str,
        target: Literal["tables", "text"],
        load_args: Dict = {}
    ):
        self._filepath = filepath
        self._target = target
        self._load_args = load_args
        self._max_pages = 9999
        
        if 'npages' in self._load_args.keys():
            npages = self._load_args.pop('npages')
            if npages:
                self._max_pages = npages

        if self._target not in ["tables", "text"]:
            raise Exception("Target must be \"tables\" or \"text\"")

    def __get_page_count(self)-> int:
        with open(self._filepath, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)

        return total_pages

    def _load(self) -> List[pd.DataFrame]:
        if self._target == "text":

            with open(self._filepath, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file, **self._load_args)
                pages = pdf_reader.pages
                
                return [ page.extract_text() for page in pages ]
        
        else:
            # Whe loop by ourself because if tablecount > 99, it will not get all the content.
            page_count = self.__get_page_count()
            tables = []

            for page_index in range(page_count):
                page_content = camelot.read_pdf(self._filepath, **self._load_args, pages=str(page_index+1)) # because camelot pages is starts at 1
                
                tables.append(page_content)

                if page_index > self._max_pages:
                    break

            return tables

    def _save(self, data: Any) -> None:
        raise DatasetError(f"Saving is unsupported")

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            target=self._target
        )

if __name__ == "__main__":
    filepath = "/home/just/Documents/mauritius-bus-compass/data_processing/mbc-data-processing/data/01_raw/route_1-20D.pdf"
    target = "tables"

    ds = PDFPageDataset(filepath, target)

    print(ds.load())