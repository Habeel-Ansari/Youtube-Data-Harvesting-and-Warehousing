## YouTube Data Harvesting and Warehousing with Python

This project provides a Python framework for automating the harvesting and warehousing of YouTube data. It utilizes the Google Data API and various libraries to fetch data on channels and videos, store it efficiently in MongoDB and MySQL, and enable analysis through a user-friendly Streamlit interface.

**Key Features:**

* **Data Harvesting:**
    * Retrieves channel details and video information using the Google Data API.
    * Extracts data like views, likes, comments, and video duration.
    * Cleans and formats the data for accurate storage.
* **Data Storage:**
    * Stores the data in both MongoDB and MySQL for flexible access and analysis.
    * Offers functions for inserting, updating, and managing data records.
* **Data Analysis:**
    * Provides a Streamlit interface for user interaction with the data.
    * Enables querying the data based on various criteria (channel name, video title, date, etc.).
    * Presents results in tables and charts for easy understanding.

**Benefits:**

* **Saves Time and Effort:** Automates data collection, eliminating manual extraction.
* **Centralized Data Storage:** Allows convenient access from different platforms.
* **Flexible Data Analysis:** Enables users to explore data based on their specific needs.
* **User-Friendly Interface:** Makes data analysis accessible to users with varying technical expertise.

**Target Users:**

* Digital marketers: Track channel performance and analyze audience engagement.
* Content creators: Gain insights into their content's reach and impact.
* Researchers: Study trends and patterns in YouTube data.

**Getting Started:**

1. **Install Requirements:** Install the required Python libraries listed in the `requirements.txt` file.
2. **Configure API Keys:** Set up your Google API key and configure MongoDB and MySQL connection details.
3. **Run the Script:** Run the `main.py` script to initiate the data harvesting and warehousing process.
4. **Access Data and Analysis:** Explore the Streamlit interface at http://localhost:8501 to query and analyze the stored data.

**Further Development:**

* Implement data visualization tools for deeper analysis.

* <img width="932" alt="image" src="https://github.com/Habeel-Ansari/Youtube-Data-Harvesting-and-Warehousing/assets/84073168/c170a2c6-c0fb-484b-be36-0514429e7a40">

* Explore sentiment analysis of video comments.
* Expand functionality to support other social media platforms.

**For more information and detailed instructions, explore the project code and documentation.**
