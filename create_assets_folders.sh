mkdir assets

# create data extracted folders
mkdir -p "assets/data-extracted"
mkdir -p "assets/data-extracted/netflix"
mkdir -p "assets/data-extracted/amazon"

# create integrated data folders
mkdir -p "assets/data-integrated"
mkdir -p "assets/data-integrated/netflix"
mkdir -p "assets/data-integrated/amazon"

# create dernomalized-data folders
mkdir -p "assets/dernomalized-data"
mkdir -p "assets/dernomalized-data/netflix"
mkdir -p "assets/dernomalized-data/amazon"

# create nomalized-data folders
mkdir -p "assets/nomalized-data"
mkdir -p "assets/nomalized-data/netflix"
mkdir -p "assets/nomalized-data/amazon"

# create source-data folders
mkdir "assets/source-databases"
mkdir "assets/source-databases/netflix"
mkdir "assets/source-databases/amazon"


chmod -R 777 assets
