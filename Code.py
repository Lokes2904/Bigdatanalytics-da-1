from mrjob.job import MRJob

class SalesByCity(MRJob):
    def mapper(self, _, line):
        """
        The mapper reads each line of the CSV file, skips the header, and extracts
        the city and total sales values. It emits city as the key and sales as the value.
        """
        try:
            # Skip the header
            if line.startswith("Invoice ID"):
                return
            
            # Parse the CSV line
            fields = line.split(",")
            city = fields[5].strip()  # City column
            sales = float(fields[9])  # Total sales column
            
            # Emit city and sales
            yield city, sales
        except (IndexError, ValueError):
            # Skip lines that are malformed or contain invalid data
            pass

    def reducer(self, city, sales):
        """
        The reducer aggregates the total sales for each city by summing up the sales values.
        """
        yield city, sum(sales)

if __name__ == "__main__":
    SalesByCity.run()
