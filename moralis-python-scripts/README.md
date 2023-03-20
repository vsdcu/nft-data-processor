# Moralis Api Connector

The project connects to Moralis.io fetching data regarding NFT Contract, NFT Transfers and OpenSea market data.

Prerequisites
--------------
- Running standalone
  - Install python 3.9 
  - Install pip

- Running on Docker
  - Install docker 
  - docker build -t moralis-connector-1.0 .
  - docker run moralis-connector-1.0 -it /bin/bash

Command to build the project
---------------------------------------
- pip install -r requirements.txt


Moralis Api Integration
----------------------

- get-contract-nfts: Retrieve the data NFT Contract based on Kaggle nft_address 
  - https://docs.moralis.io/web3-data-api/evm/reference/get-contract-nfts 
  
- get-nft-contract-transfers: Retrieve the transactions for a NFT Contract
  - https://docs.moralis.io/web3-data-api/evm/reference/get-nft-contract-transfers

- get-nft-trades: Retrieve the marked value for OpenSea.io
  - https://docs.moralis.io/web3-data-api/evm/reference/get-nft-trades 
