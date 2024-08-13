# DataMate

**DataMate** is a tool that allows you to export data from MySQL databases to CSV files and migrate data from CSV files back into a MySQL database. This project is built with Flask and provides a web interface for easy data management.

## Features

- **Export MySQL Data to CSV**: Select tables from a MySQL database and export their data to CSV files.
- **Import CSV Data to MySQL**: Upload a CSV file and migrate its data to a specified MySQL table.
- **Dynamic Table Creation**: Automatically create tables if they do not exist in the database during import.
- **Custom Field Mapping**: Map CSV fields to database columns during import.

## Prerequisites

Before running the application, ensure you have the following installed:

- [Python 3.6+](https://www.python.org/downloads/)
- [MySQL Server](https://dev.mysql.com/downloads/)
- [pip](https://pip.pypa.io/en/stable/) (Python package installer)

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/srikantsingh673/DataMate.git
   cd DataMate
