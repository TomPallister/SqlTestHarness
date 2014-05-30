using System.Configuration;

namespace Fourth.Sql.Test.Harness.Configuration
{
    public class TempDbSettingsSection : ConfigurationSection
    {
        [ConfigurationProperty("sourceDb")]
        public DbElement SourceDb
        {
            get { return (DbElement) this["sourceDb"]; }
            set { this["sourceDb"] = value; }
        }

        [ConfigurationProperty("targetDb")]
        public DbElement TargetDb
        {
            get { return (DbElement) this["targetDb"]; }
            set { this["targetDb"] = value; }
        }
    }
}