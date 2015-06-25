# Reward-Style-Backend-Test
Nginx, two Tornado Instances, ElasticSeach db backend.

(Remove prior) Load and prime feed for advertiser with:<br />
http://localhost:8080/products?advertiser=100%20Percent%20Pure

Supported Params:<br />
limit<br />
offset<br />
priceMin<br />
priceMax<br />
keywords

http://localhost:8080/search?limit=100<br />
http://localhost:8080/search?limit=100&priceMax=10<br />
http://localhost:8080/search?limit=100&priceMin=10&priceMax=100&offset=10<br />
http://localhost:8080/search?limit=100&priceMin=10&priceMax=100&keywords=red<br />
http://localhost:8080/search?limit=100&priceMin=10&priceMax=1000&keywords=red,black<br />
