# Machine Learning applied to Idealista

This repository contains all the code used to build a machine learning model to predict the price of the rent of a house in the Province of Valencia, Spain. 

The main objetive was to predict the rent given a set of features and to compare the results with the real price of the rent published in the portal of Idealista to make a decision about to rent or not a house.

This model is deployed in a web application using Flask and Heroku. The web application is available in the following link: https://ml-idealista.herokuapp.com

## ¿How it works?

1. The first thing you have to do is to search a house in the portal of Idealista and copy the link of the house. *For example*, the link of the house is: https://www.idealista.com/inmueble/123456789/
2. Paste the link in the web application and click on the button "Predecir".
3. Done! You will see the predicted price of the rent and the real price of the rent below...

## Requirements 📋 and Installation 🔧

This project was developed using Python 3.7.6. The requirements are in the file requirements.txt. You can install all the requirements using the following command:

``` pip install -r requirements.txt ```

And you will need to have .env file in the root with your credentials to access to the API of Idealista. The .env file should have the following content:

```
API_IDEALISTA_KEY=API_IDEALISTA_SECRET
```

This is needed to make the request to the API of Idealista and get the information of the house that you want to predict.

## Built with 🛠️

* [Python](https://www.python.org/) - The programming language used

* [Flask](https://flask.palletsprojects.com/en/1.1.x/) - The web framework used

* [Heroku](https://www.heroku.com/) - The cloud platform used

* [Jupyter Notebook](https://jupyter.org/) - The environment used to develop the model

* [Scikit-learn](https://scikit-learn.org/stable/) - The machine learning library used


## Conclusion 📖

The model has a good performance but not the best, this could be because there's not trained with enough data limited by my API key that only allows 200 requests per month and I have been working with this project around 2 months and i can't have historical data of the houses, so in resume, i've been working on a houses published in the last 2 months (Mostly summer rentals). So this could be the reason why the model has a good performance but not the best. The **RMSE** is around 460€.

Maybe in the future I will try to improve the model with more data and more features like for example analyze the photos given in the ad or time that the house has been published in the portal.

## Authors ✒️

* **Carlos I. Cabruja Rodil** [carloscabruja](https://github.com/carloscabruja) 

## License 📄

This project is built under the license (MIT License) - see [LICENSE.md](LICENSE.md) for details

## Thanks to... 🙏

* [Idealista](https://www.idealista.com/) - The portal used to get the data
* [Idealista API](https://developers.idealista.com/) - For giving me access to the API of Idealista
* [The_Bridge](https://thebridge.tech/) - For giving me the opportunity to learn Data Science
* [Marco Russo](https://github.com/marcusRB) - For Teaching me Data Science
* [Juan Maniglia](https://github.com/JuanManiglia) - For Teaching me Data Science
