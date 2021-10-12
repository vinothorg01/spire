#Install python3 and pip
brew install python3 
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py

#Install virtual env and export quer    
sudo -H pip3 install virtualenv
sudo -H pip3 install virtualenvwrapper --ignore-installed six
# export  of python virtual environments 
export WORKON_HOME=$HOME/.virtualenvs

source `which virtualenvwrapper.sh`

mkvirtualenv --python=`which python3` spire-cli
source $HOME/.virtualenvs/spire-cli/bin/activate

#download source code and install
rm -rf ~/.spire-codebase/
mkdir ~/.spire-codebase/ 
cd ~/.spire-codebase/

git clone https://github.com/CondeNast/spire.git
cd spire/

cd include/datasci-common/
git clone https://github.com/CondeNast/datasci-common.git
pip install ./datasci-common/python/

cd ../../
cd include/kalos/
git clone https://github.com/CondeNast/kalos.git
pip install ./kalos/

cd ../
cd inlcude/spire-astronomer/
git clone https://github.com/CondeNast/spire-astronomer.git

cd ../
pip install -r requirements.txt
pip install .
pip install -e ./cli

