from typing import Dict, Any
from kedro.io import AbstractDataset
from typing import List

import PyPDF2
import os
from pathlib import Path

class PDFDataset(AbstractDataset):
    """
    Dataset that only get the tables of a PDF.
    Return the tables of each pages in an array
    """
    def __init__(
        self,
        filepath: str,
    ):
        self._filepath = Path(filepath)


    def _load(self) -> List[str]:
        return PyPDF2.PdfReader(self._filepath)

    def _save(self, data: Dict[str, PyPDF2.PageObject]) -> None:
        pdf_writer = PyPDF2.PdfWriter(self._filepath)

        [ pdf_writer.add_page(page) for page in data ]

        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self._filepath, 'wb') as new_pdf:
            pdf_writer.write(new_pdf)

        
    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath
        )

if __name__ == "__main__":
    filepath = "/home/just/Documents/mauritius-bus-compass/data_processing/mbc-data-processing/data/01_raw/route_1-20D.pdf"
    target = "tables"

    ds = PDFDataset(filepath, target)

    print(ds.load())