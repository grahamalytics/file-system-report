Get-ChildItem -path F:\Alteryx\02Backup\AEO -recurse | Where-Object { 
    ($_.Length / 1KB) -gt 1
} | Select-Object LastWriteTime,Length,FullName,Name,@{N='Owner';E={$_.GetAccessControl().Owner}}