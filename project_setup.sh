#create assets folder
mkdir assets

# create source-data folders
mkdir "assets/source-databases"
mkdir "assets/source-databases/netflix"
mkdir "assets/source-databases/amazon"

# create data-extracted folders
mkdir -p "assets/data-extracted"
mkdir -p "assets/data-extracted/netflix"
mkdir -p "assets/data-extracted/amazon"

# create dw-tables folders
mkdir -p "assets/dw-tables"
mkdir -p "assets/dw-tables/netflix"
mkdir -p "assets/dw-tables/amazon"

# create integrated data folders
mkdir -p "assets/data-integrated"

# Give full permission acesson assets folder and its children
chmod -R 777 assets

#create needed enviorments variables to run this projects
source .env

#create python env , activate it and install project dependencies...
python3 -m venv fivegflix-env
source "fivegflix-env/bin/activate"

echo ""
echo "Project setup ready!"