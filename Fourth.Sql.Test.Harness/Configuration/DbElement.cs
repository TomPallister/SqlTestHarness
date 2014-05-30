using System;
using System.Configuration;
using System.IO;
using System.Reflection;

namespace Fourth.Sql.Test.Harness.Configuration
{
    public class DbElement : ConfigurationElement
    {
        [ConfigurationProperty("serverInstance", DefaultValue = @"(local)\Test", IsRequired = false)]
        public string ServerInstance
        {
            get { return (string) this["serverInstance"]; }
            set { this["serverInstance"] = value; }
        }

        [ConfigurationProperty("databaseName", DefaultValue = "Set_Test", IsRequired = false)]
        public string DatabaseName
        {
            get { return (string) this["databaseName"]; }
            set { this["databaseName"] = value; }
        }

        [ConfigurationProperty("userName")]
        public string UserName
        {
            get { return (string) this["userName"]; }
            set { this["userName"] = value; }
        }

        [ConfigurationProperty("password")]
        public string Password
        {
            get { return (string) this["password"]; }
            set { this["password"] = value; }
        }

        [ConfigurationProperty("dataFilePath")]
        public string DataFilePath
        {
            get
            {
                if (string.IsNullOrWhiteSpace((string) this["dataFilePath"]))
                {
                    string directoryPath = GetExecutingDirectory();
                    string fileName = string.Format("{0}.mdf", DatabaseName);

                    this["dataFilePath"] = Path.Combine(directoryPath, fileName);
                }
                return (string) this["dataFilePath"];
            }
            set { this["dataFilePath"] = value; }
        }

        [ConfigurationProperty("logFilePath")]
        public string LogFilePath
        {
            get
            {
                if (string.IsNullOrWhiteSpace((string) this["logFilePath"]))
                {
                    string directoryPath = GetExecutingDirectory();
                    string fileName = string.Format("{0}_log.ldf", DatabaseName);

                    this["logFilePath"] = Path.Combine(directoryPath, fileName);
                }
                return (string) this["logFilePath"];
            }
            set { this["logFilePath"] = value; }
        }

        [ConfigurationProperty("tempFilePath")]
        public string TempFilePath
        {
            get
            {
                if (string.IsNullOrWhiteSpace((string) this["tempFilePath"]))
                {
                    string directoryPath = GetExecutingDirectory();
                    string fileName = string.Format("{0}.sql", Guid.NewGuid());

                    this["tempFilePath"] = Path.Combine(directoryPath, fileName);
                }
                return (string) this["tempFilePath"];
            }
            set { this["tempFilePath"] = value; }
        }

        public override bool IsReadOnly()
        {
            return false;
        }

        private string GetExecutingDirectory()
        {
            string path = Assembly.GetExecutingAssembly().GetName().CodeBase;
            return Path.GetDirectoryName(new Uri(path).LocalPath);
        }
    }
}