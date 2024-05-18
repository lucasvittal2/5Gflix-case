mkdir assets

# create data extracted folders
mkdir -p "assets/data-extracted"
mkdir -p "assets/data-extracted/netflix"
mkdir -p "assets/data-extracted/amazon"

# create integrated data folders
mkdir -p "assets/data-integrated"
mkdir -p "assets/data-integrated/netflix"
mkdir -p "assets/data-integrated/amazon"

# create dw tables folders
mkdir -p "assets/dw-tables"
mkdir -p "assets/dw-tables/netflix"
mkdir -p "assets/dw-tables/amazon"



# create source-data folders
mkdir "assets/source-databases"
mkdir "assets/source-databases/netflix"
mkdir "assets/source-databases/amazon"


chmod -R 777 assets
