# Data Science Capstone

Trends in Technology Conferences

Evan Carraway

April 6, 2020

## Objective

This is a conference aggregation and analysis project on Azure prepared as part of the George Washington University Data Science Masters Program. This project seeks to address multiple challenges associated with finding, aggregating and analyzing conferences. This project also seeks to help visualize trends in technology and security conferences and identify potentially disruptive technologies.

## Abstract

The pace of technological change has become an organizational challenge itself. Ray Kurzweil argued that with the rate of technology acceleration we see now, we will experience 20,000 years of progress in the 21st century as opposed to 100 years (Alier, 2017). Today we see technology disruption from Internet of Things (IoT), Artificial Intelligence (AI), Autonomous Systems and Distributed Ledger technologies (Panetta, 2019). What this means is that organizations need to be adaptable to potentially disruptive technology in order to remain successful. Research studies and the market have borne out that companies that are open the opportunities and disruption of technology are ultimately more successful (Birkinshaw, 2018).

Effective responses involve multiple steps including building awareness, capacity and scaling for technology adoption (Birkinshaw, 2018).  While in the past, traditional investment approaches towards technology forecasting would have been effective or at least adequate for achieving awareness of potential disruptive technologies, this may no longer be the case with the rate of change. However, data science and machine learning/artificial intelligence (ML/AL) may offer solutions to mitigate the deluge of new technologies and help determine impact and scale of changes faced by organizations.

The goal of this project will be to demonstrate and evaluate end-to-end techniques that can be used find disruptive technology topics using data/text mining, natural language processing (NLP), cloud computing and data visualization. It will rely primarily on Python data mining libraries and web scraping tools such as Selenium and BeautifulSoup; machine learning / deep learning frameworks such as Scikit-Learn, Keras and Tensorflow; and visualization libraries and web frameworks including Seaborn, Plotly, Flask and Bootstrap HTML/CSS/JavaScript.

## Tools Used

The project is presented in the form of an interactive HTML page using Python Flask, Bootstrap, CSS, client-side JavaScript such as JQuery, D3.js, Leaflet, DataTable and other applicable visualization libraries. It also leveraged a custom scraping and retrieval framework with Python, Pandas, BeautifulSoup, Selenium and the Requests library for retrieval and processing. Source code management leveraged GitHub and Visual Studio Code.

## Architecture

This project leveraged Azure infrastructure for multiple elements, including the main backend database architecture, which used Cosmos DB. Cosmos DB is a multi-modal NoSQL database as a service (DBaaS) which allows for rapid prototyping of new datastructures and quick secure integration with other systems. For the hosting and integration of the Python Flask, we used Azure Web Apps which is a fully-managed web hosting platform supporting continuous integration/continuous deployment (CI/CD), application monitoring and metrics.

![architecture](/static/architecture.png)

## References

Alier, M., & Casany, M. J. (2017). The need for interdisciplinary research on exponential technologies and sustainability. Proceedings of the 5th International Conference on Technological Ecosystems for Enhancing Multiculturality - TEEM 2017. doi: 10.1145/3144826.3145377

Baer, D. (2015, May 27). Google's genius futurist has one theory that he says will rule the future - and it's a little terrifying. Retrieved February 10, 2020, from <https://www.businessinsider.com/ray-kurzweil-law-of-accelerating-returns-2015-5>

Berman, A. E., & Dorrier, J. (2019, July 10). Technology Feels Like It's Accelerating - Because It Actually Is. Retrieved February 10, 2020, from <https://singularityhub.com/2016/03/22/technology-feels-like-its-accelerating-because-it-actually-is/>

Birkinshaw, J., Visnjic, I., & Best, S. (2018). Responding to a Potentially Disruptive Technology: How Big Pharma Embraced Biotechnology. California Management Review, 60(4), 74–100. doi: 10.1177/0008125618778852

Brik, B., Bettayeb, B., Sahnoun, M. H., & Duval, F. (2019). Towards Predicting System Disruption in Industry 4.0: Machine Learning-Based Approach. Procedia Computer Science, 151, 667–674. doi: 10.1016/j.procs.2019.04.089

Cag, D. (2019, December 29). 11 Awesome Disruptive Technology Examples 2020 (MUST READ). Retrieved February 10, 2020, from <https://richtopia.com/emerging-technologies/11-disruptive-technology-examples>

Harrington, L. (2018, November 20). 5 Disruptive Technologies Shaping Our Future. Retrieved February 10, 2020, from <https://www.iotforall.com/5-disruptive-technologies-shaping-our-future/>

National Research Council. (2010). Persistent forecasting of disruptive technologies: report 2. Washington: National Academies Press.

Paap, J., & Katz, R. (2004). Anticipating disruptive innovation. IEEE Engineering Management Review, 32(4), 74–85. doi: 10.1109/emr.2004.25138 

Panetta, K. (2019, October 21). Gartner Top 10 Strategic Technology Trends for 2020. Retrieved February 10, 2020, from <https://www.gartner.com/smarterwithgartner/gartner-top-10-strategic-technology-trends-for-2020/>

Schuelke-Leech, B.-A. (2018). A model for understanding the orders of magnitude of disruptive technologies. Technological Forecasting and Social Change, 129, 261–274. doi: 10.1016/j.techfore.2017.09.033

World Customs Organization. (2019, June). Study Report on Disruptive Technologies. Retrieved February 10, 2020, from <http://www.wcoomd.org/-/media/wco/public/global/pdf/topics/facilitation/ressources/permanent-technical-committee/223-224/pc_0541_annex_e.pdf?la=en>