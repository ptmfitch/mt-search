sudo yum install pip -y
sudo yum install git -y

pip install pymongo

git clone https://github.com/ptmfitch/mt-search

cd mt-search
cp _secrets.example.py _secrets.py

nano _secrets.py