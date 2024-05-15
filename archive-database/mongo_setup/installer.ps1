
do {
  $operation = Read-Host "Would you like to [i]nstall or [r]emove mongodb"
} until (($operation -eq "i") -or ($operation -eq "r"))

Write-Host "Leave blank for default"

Switch ($operation)
{
    "i" {
        if (($install_dir = Read-Host -Prompt "Install directory [f:\FEWSArchiveMongoDB\install]") -eq "") {$install_dir= "f:\FEWSArchiveMongoDB\install"}
        if (($data_dir = Read-Host -Prompt "Data directory [f:\FEWSArchiveMongoDB\data]") -eq "") {$data_dir= "f:\FEWSArchiveMongoDB\data"}
        if (($log_dir = Read-Host -Prompt "Log directory [f:\FEWSArchiveMongoDB\log]") -eq "") {$log_dir= "f:\FEWSArchiveMongoDB\log"}
        if (($service_name = Read-Host -Prompt "Mongo Service Name [MongoDB]") -eq "") {$service_name= "MongoDB"}
        if (($port = Read-Host -Prompt "Port Number [27017]") -eq "") {$port= "27017"}
        powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 install -install_dir $install_dir -data_dir $data_dir -log_dir $log_dir -service_name $service_name -port $port
    }
    "r" {
        if (($install_dir = Read-Host -Prompt "Install directory [f:\FEWSArchiveMongoDB\install]") -eq "") {$install_dir= "f:\FEWSArchiveMongoDB\install"}
        if (($service_name = Read-Host -Prompt "Mongo Service Name [MongoDB]") -eq "") {$service_name= "MongoDB"}
        powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 remove -install_dir $install_dir -service_name $service_name
    }
}
