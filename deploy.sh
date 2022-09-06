poetry run etl-cli create --resource resources  --overwrite
scp -r ./sql  sz03@master:/home/sz03/
ssh sz03@master "source ~/.bash_profile && hdfs dfs -put -f  ./sql/* /etl/sql && exit"

