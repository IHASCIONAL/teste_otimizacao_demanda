from frontend import (
    PageConfig, Header, OrdersReader, MessageDisplay, ResultDisplay
)

from backend import DataProcessor, FixingTopForecastingFile

import streamlit as st

def main():
    page_config = PageConfig(page_title="Documentação", layout="wide")
    header = Header(title="Em desenvolvimento!", subtitle=None)
    header.display_header()


if __name__ == "__main__":
    main()