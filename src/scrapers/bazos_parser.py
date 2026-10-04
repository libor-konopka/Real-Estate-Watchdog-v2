from bs4 import BeautifulSoup


class BazosDOMParser:
    """
    Analyzes the HTML structure of the advertisement detail on the Bazoš portal.
    Isolates the use of CSS selectors from the rest of the application.
    """

    def __init__(self, html_content: str) -> None:
        self.soup = BeautifulSoup(html_content, "lxml")

    def _extract_title(self) -> str:
        if title_element := self.soup.find("h1", class_="nadpisdetail"):
            return title_element.get_text(strip=True)
        return ""

    def _extract_description(self) -> str:
        if desc_element := self.soup.find("div", class_="popisdetail"):
            return desc_element.get_text(separator=" ", strip=True)
        return ""

    def _extract_table_row_value(self, label: str) -> str:
        """
        Extracts text from a table cell that immediately follows a cell
        containing the specified label.
        """
        for element in self.soup.find_all(["td", "th"]):
            if element.get_text(strip=True).lower() == label:
                for sibling in element.find_next_siblings("td"):
                    if text := sibling.get_text(separator=" ", strip=True):
                        return text
        return ""

    def _extract_price(self) -> str:
        return self._extract_table_row_value("cena:")

    def _extract_location(self) -> str:
        return self._extract_table_row_value("lokalita:")

    def parse_all(self) -> dict[str, str]:
        """
        Runs all extraction methods and returns a bronze data dictionary
        ready for insertion into the BazosRawInput model.
        """
        return {
            "raw_title": self._extract_title(),
            "raw_description": self._extract_description(),
            "raw_price": self._extract_price(),
            "raw_location": self._extract_location(),
        }