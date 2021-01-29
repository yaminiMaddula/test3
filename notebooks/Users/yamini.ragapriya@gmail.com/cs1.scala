// Databricks notebook source
import scala.util.control._

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "ed31fd6a-c2e8-4b09-a684-7f06a21ee0be",
  "fs.azure.account.oauth2.client.secret" -> "rVoh~bKe9I.o1OL6_x.7G3Q2_fX9ZS9VYt",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/53f6bcef-ff77-43fd-b8e8-fa93bcea2c67/oauth2/token")


// COMMAND ----------

val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
val outer = new Breaks;

outer.breakable {
  for(mount <- mounts) {
    if(mount.mountPoint == mountPath) {
      isExist = true;
      outer.break;
    }
  }
}

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudy2@yamadls.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}