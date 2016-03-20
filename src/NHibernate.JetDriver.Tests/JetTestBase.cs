using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using NHibernate.ByteCode.Castle;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Engine;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using NHibernate.Tool.hbm2ddl;
using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.JetDriver.Tests
{
    public abstract class JetTestBase
    {
        private readonly JetDriver jetDriver = new JetDriver();
        private readonly SqlType[] dummyParameterTypes = { };
        private readonly Configuration configuration;
        private readonly ISessionFactory factory;
        private readonly ISessionFactoryImplementor factoryImpl;

        protected JetTestBase()
            : this(false)
        {
        }

        protected JetTestBase(bool autoCreateTables)
        {
            configuration = new Configuration()
                .SetProperty(Environment.ProxyFactoryFactoryClass, typeof(ProxyFactoryFactory).AssemblyQualifiedName)
                .SetProperty(Environment.Dialect, typeof(JetDialect).AssemblyQualifiedName)
                .SetProperty(Environment.ConnectionDriver, typeof(JetDriver).AssemblyQualifiedName)
                .SetProperty(Environment.ConnectionProvider, typeof(DriverConnectionProvider).FullName)
                .SetProperty(Environment.ShowSql, "true")
                .SetDefaultNamespace("NHibernate.JetDriver.Tests.Entities")
                .SetProperty(Environment.ConnectionString, string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};", DataFile));

            AddMappings();
            AddEntities();

            factory = configuration.BuildSessionFactory();
            factoryImpl = (ISessionFactoryImplementor)factory;

            if (autoCreateTables)
            {
                CreateTables();
                InsertSampleData();
            }
        }

        /// <summary>
        /// Insert dummy data for the tests
        /// </summary>
        private void InsertSampleData()
        {
            var conn = new System.Data.OleDb.OleDbConnection(configuration.GetProperty(Environment.ConnectionString));
            conn.Open();
            new JetDbCommand("BEGIN TRANSACTION;", conn).ExecuteNonQuery();
            new JetDbCommand("DELETE FROM thing;", conn).ExecuteNonQuery();
            new JetDbCommand("INSERT INTO thing VALUES (1, 'Car');", conn).ExecuteNonQuery();
            new JetDbCommand("INSERT INTO thing VALUES (2, 'Cat');", conn).ExecuteNonQuery();
            new JetDbCommand("INSERT INTO thing VALUES (3, 'Apple');", conn).ExecuteNonQuery();
            new JetDbCommand("INSERT INTO thing VALUES (4, 'Albacete');", conn).ExecuteNonQuery();
            new JetDbCommand("INSERT INTO thing VALUES (5, 'Foo');", conn).ExecuteNonQuery();
            new JetDbCommand("COMMIT;", conn).ExecuteNonQuery();
        }

        /// <summary>
        /// Assembly containing the mapping files (.hbml files)
        /// </summary>
        protected virtual Assembly MappingsAssembly
        {
            get { return GetType().Assembly; }
        }

        /// <summary>
        /// List of entity .hbm mappings to be added 
        /// automatically on startup
        /// </summary>
        protected virtual IList<string> Mappings
        {
            get { return new List<string>(); }
        }

        private void AddEntities()
        {
            foreach (var type in EntityTypes)
            {
                configuration.AddClass(type);
            }
        }

        private void AddMappings()
        {
            foreach (var file in Mappings)
            {
                configuration.AddResource(MappingsAssembly.GetName().Name + "." + file, MappingsAssembly);
            }
        }

        protected Configuration Configuration
        {
            get { return configuration; }
        }

        protected virtual void CreateTables()
        {
            new SchemaExport(configuration).Create(true, true);
        }

        protected virtual void DropTables()
        {
            new SchemaExport(configuration).Drop(true, true);
        }

        protected virtual IList<System.Type> EntityTypes
        {
            get { return new List<System.Type>(); }
        }

        private static string DataFile
        {
            get { return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "JetTests.db"); }
        }

        protected string GetTransformedSql(string sqlQuery)
        {
            var sql = SqlString.Parse(sqlQuery);
            var command = jetDriver.GenerateCommand(CommandType.Text, sql, dummyParameterTypes);

            return command.CommandText;
        }

        protected ISessionFactory SessionFactory
        {
            get { return factory; }
        }

        protected ISessionFactoryImplementor SessionFactoryImpl
        {
            get { return factoryImpl; }
        }
    }
}