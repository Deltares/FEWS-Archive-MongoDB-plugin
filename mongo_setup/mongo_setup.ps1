param(
	[string]$operation = "",
	[string]$install_dir = "",
	[string]$data_dir = "",
	[string]$log_dir = "",
	[string]$service_name = "",
	[string]$port = ""
)
$ErrorActionPreference = "Stop"

Switch ($operation)
{
	"install" {
		If($install_dir -eq "" -or $data_dir -eq "" -or $log_dir -eq "" -or $service_name -eq "" -or $port -eq ""){
			Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 install -install_dir install_dir -data_dir data_dir -log_dir log_dir -service_name service_name -port port"
			Write-Output "-install_dir=$install_dir"
			Write-Output "-data_dir=$data_dir"
			Write-Output "-log_dir=$log_dir"
			Write-Output "-service_name=$service_name"
			Write-Output "-port=$port"
			Break
		}
		Write-Output "Creating Directories..."
		If(!(Test-Path $install_dir)){md $install_dir | Out-Null}
		If(!(Test-Path $data_dir)){md $data_dir | Out-Null}
		If(!(Test-Path $log_dir)){md $log_dir | Out-Null}
		
		If (Get-Service $service_name -ErrorAction SilentlyContinue) {If ((Get-Service $service_name).Status -eq "Running") {
			Write-Output "Stopping Service..." 
			Stop-Service $service_name -Confirm:$false}}
		
		If (Get-Service $service_name -ErrorAction SilentlyContinue) {
			Write-Output "Removing Service..."
			& sc.exe delete $service_name | Out-Null}
		
		Write-Output "Installing MongoDB..."
		Expand-Archive .\mongodb\mongodb-windows-x86_64-4.4.3.zip -DestinationPath $install_dir -Force
		Copy-Item -Path $install_dir\mongodb-win32-x86_64-windows-4.4.3\* $install_dir -Recurse	-Force
		Remove-Item -Path $install_dir\mongodb-win32-x86_64-windows-4.4.3 -Recurse -Force -Confirm:$false -ErrorAction Ignore
		Remove-Item -Path $install_dir\bin\Install-Compass.ps1 -Confirm:$false -ErrorAction Ignore

		Write-Output "Writing Configuration..."
		$output = @"
storage:
  dbPath: $data_dir
  journal:
    enabled: true
systemLog:
  destination: "file"
  path: $log_dir\mongod.log
net:
  bindIp: 0.0.0.0
  port: $port
security:
  authorization: "disabled"
"@
		Out-File -FilePath $install_dir\bin\mongod.cfg -InputObject $output

		Write-Output "Installing Service..."
		& "$install_dir\bin\mongod.exe" --install --config $install_dir\bin\mongod.cfg --serviceName $service_name --serviceDisplayName "MongoDB Server ($service_name)" | Out-Null
		
		Write-Output "Starting Service..."
		Start-Service $service_name -Confirm:$false
		
		Start-Sleep -Seconds 10
		
		Break
	}
	"remove" {
		If($install_dir -eq "" -or $service_name -eq ""){
			Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 remove -install_dir install_dir -service_name service_name"
			Write-Output "-install_dir=$($install_dir)"
			Write-Output "-service_name=$($service_name)"
			Break
		}
		If (Get-Service $service_name -ErrorAction SilentlyContinue) {If ((Get-Service $service_name).Status -eq "Running") {
			Write-Output "Stopping Service..." 
			Stop-Service $service_name -Confirm:$false}}
		
		If (Get-Service $service_name -ErrorAction SilentlyContinue) {
			Write-Output "Removing Service..."
			& sc.exe delete $service_name | Out-Null}
		
		If (Test-Path $install_dir){
			Write-Output "Removing MongoDB..."
			Remove-Item $install_dir -Recurse -Confirm:$false -ErrorAction Ignore}
			
		Break
	}
	Default {
		Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 install -install_dir install_dir -data_dir data_dir -log_dir log_dir -service_name service_name -port port"
		Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 install -install_dir f:\test\install -data_dir f:\test\data -log_dir f:\test\log -service_name TestFEWSArchiveMongoDB -port 27019"

		Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 remove -install_dir install_dir -service_name service_name"
		Write-Output "powershell.exe -ExecutionPolicy Bypass -File mongo_setup.ps1 remove -install_dir f:\test\install -service_name TestFEWSArchiveMongoDB"
	}
}
Write-Output "[$($MyInvocation.BoundParameters.Keys | %{"-$_ $($MyInvocation.BoundParameters[$_])"})]"
Write-Output "Complete!"