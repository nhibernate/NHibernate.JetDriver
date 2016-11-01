using System;
using System.Data;

using NHibernate.Dialect.Function;
using NHibernate.SqlCommand;

using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.JetDriver
{
    /// <summary>
    /// Dialect for Jet database engine.
    /// </summary>
    public class JetDialect : Dialect.Dialect
    {
        /// <summary>
        /// 
        /// </summary>
        public JetDialect()
            : base()
        {
            //BINARY 1 byte per character Any type of data may be stored in a field of this type. No translation of the data (for example, to text) is made. How the data is input in a binary field dictates how it will appear as output. 
            //BIT 1 byte Yes and No values and fields that contain only one of two values. 
            //TINYINT 1 byte An integer value between 0 and 255. 
            //MONEY 8 bytes A scaled integer between
            //� 922,337,203,685,477.5808 and 922,337,203,685,477.5807. 
            //DATETIME
            //(See DOUBLE) 8 bytes A date or time value between the years 100 and 9999. 
            //UNIQUEIDENTIFIER 128 bits A unique identification number used with remote procedure calls. 
            //REAL 4 bytes A single-precision floating-point value with a range of � 3.402823E38 to � 1.401298E-45 for negative values, 1.401298E-45 to 3.402823E38 for positive values, and 0. 
            //FLOAT 8 bytes A double-precision floating-point value with a range of � 1.79769313486232E308 to � 4.94065645841247E-324 for negative values, 4.94065645841247E-324 to 1.79769313486232E308 for positive values, and 0. 
            //SMALLINT 2 bytes A short integer between � 32,768 and 32,767. (See Notes) 
            //INTEGER 4 bytes A long integer between � 2,147,483,648 and 2,147,483,647. (See Notes) 
            //DECIMAL 17 bytes An exact numeric data type that holds values from 1028 - 1 through - 1028 - 1. You can define both precision (1 - 28) and scale (0 - defined precision). The default precision and scale are 18 and 0, respectively. 
            //TEXT 2 bytes per character (See Notes) Zero to a maximum of 2.14 gigabytes. 
            //IMAGE As required Zero to a maximum of 2.14 gigabytes. Used for OLE objects. 
            //CHARACTER 

            //Although it is clearly stated in MS Access documentation, that Jet engine supports TINYINT, it is actually not true.
            //Byte size number datatype is called BYTE.

            RegisterColumnType(DbType.AnsiStringFixedLength, "CHAR(255)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 255, "CHAR($l)");
            RegisterColumnType(DbType.AnsiString, "TEXT(255)");
            RegisterColumnType(DbType.AnsiString, 255, "TEXT($l)");
            //RegisterColumnType(DbType.AnsiString, 1073741823, "MEMO");
            RegisterColumnType(DbType.AnsiString, 1073741823, "MEMO");
            RegisterColumnType(DbType.Binary, "IMAGE");
            //RegisterColumnType( DbType.Binary, 8000, "VARBINARY($i)" );
            RegisterColumnType(DbType.Binary, 2147483647, "IMAGE");
            RegisterColumnType(DbType.Boolean, "BIT");
            RegisterColumnType(DbType.Byte, "BYTE");
            RegisterColumnType(DbType.Currency, "MONEY");
            RegisterColumnType(DbType.Date, "DATETIME");
            RegisterColumnType(DbType.DateTime, "DATETIME");
            // TODO: figure out if this is the good way to fix the problem
            // with exporting a DECIMAL column
            // NUMERIC(precision, scale) has a hardcoded precision of 19, even though it can range from 1 to 38
            // and the scale has to be 0 <= scale <= precision.
            // I think how I might handle it is keep the type="Decimal(29,5)" and make them specify a 
            // sql-type="decimal(20,5)" if they need to do that.  The Decimal parameter and ddl will get generated
            // correctly with minimal work.
            RegisterColumnType(DbType.Decimal, "DECIMAL(19,5)");
            RegisterColumnType(DbType.Decimal, 19, "DECIMAL(19, $l)");
            RegisterColumnType(DbType.Double, "FLOAT");
            RegisterColumnType(DbType.Guid, "GUID");
            RegisterColumnType(DbType.Int16, "SMALLINT");
            RegisterColumnType(DbType.Int32, "INT");
            RegisterColumnType(DbType.Int64, "REAL");
            RegisterColumnType(DbType.Single, "REAL");
            RegisterColumnType(DbType.StringFixedLength, "CHAR(255)");
            RegisterColumnType(DbType.StringFixedLength, 255, "CHAR($l)");
            RegisterColumnType(DbType.StringFixedLength, 1073741823, "MEMO");
            RegisterColumnType(DbType.String, "TEXT(255)");
            RegisterColumnType(DbType.String, 255, "TEXT($l)");
            RegisterColumnType(DbType.String, 1073741823, "MEMO");
            //RegisterColumnType(DbType.String, 1073741823, "MEMO");
            RegisterColumnType(DbType.Time, "DATETIME");

            RegisterFunction("upper", new StandardSQLFunction("ucase"));
            RegisterFunction("lower", new StandardSQLFunction("lcase"));

            //although theoretically Access should support outer joins, it has some severe 
            //limitations on complexity of the SQL statements, so we better switch it off.
            DefaultProperties[Environment.MaxFetchDepth] = "0";
            DefaultProperties[Environment.PrepareSql] = "false";

            DefaultProperties[Environment.ConnectionDriver] = "NHibernate.Driver.JetDriver";
        }

        /// <summary>
        /// The name of the SQL function that transforms a string to lowercase
        /// </summary>
        public override string LowercaseFunction
        {
            get { return "lcase"; }
        }

        public override string AddColumnString
        {
            get { return "add"; }
        }

        public override string NullColumnString
        {
            get { return " null"; }
        }

        public override bool QualifyIndexName
        {
            get { return false; }
        }

        public override string ForUpdateString
        {
            get { return string.Empty; }
        }

        /// <summary></summary>
        public override bool SupportsIdentityColumns
        {
            get { return true; }
        }

        /// <summary></summary>
        public override string GetIdentitySelectString(string identityColumn, string tableName, DbType type)
        {
            return "select @@identity";
        }

        /// <summary>
        /// Access uses a COUNTER type for identity columns
        /// </summary>
        public override string IdentityColumnString
        {
            get { return "COUNTER"; }
        }

        /// <summary>
        /// Whether this dialect has a seperate identity data type
        /// </summary>
        public override bool HasDataTypeInIdentityColumn
        {
            get { return false; }
        }

        /// <summary></summary>
        public override string NoColumnsInsertString
        {
            get { return "DEFAULT VALUES"; }
        }

        /// <summary></summary>
        public override char CloseQuote
        {
            get { return '`'; }
        }

        /// <summary></summary>
        public override char OpenQuote
        {
            get { return '`'; }
        }

        /// <summary>
        /// Does this Dialect have some kind of <c>LIMIT</c> syntax?
        /// </summary>
        /// <value>False.</value>
        public override bool SupportsLimit
        {
            get { return true; }
        }

        /// <summary>
        /// Does this Dialect support an offset?
        /// </summary>
        public override bool SupportsLimitOffset
        {
            get { return false; }
        }

        /// <summary>
        /// Can parameters be used for a statement containing a LIMIT?
        /// </summary>
        public override bool SupportsVariableLimit
        {
            get { return false; }
        }


        /// <summary>
        /// MS Access and SQL Server support limit. This implementation has been made according the MS Access syntax
        /// </summary>
        /// <param name="querySqlString">The original query</param>
        /// <param name="offset">Specifies the number of rows to skip, before starting to return rows from the query expression.</param>
        /// <param name="limit">Is used to limit the number of results returned in a SQL statement</param>
        /// <returns>Processed query</returns>
        public override SqlString GetLimitString(SqlString querySqlString, int offset, int limit)
        {
            return querySqlString.Replace("select", string.Format("select top {0}", limit));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        /// <remarks>
        /// MsSql does not require the OpenQuote to be escaped as long as the first char
        /// is an OpenQuote.
        /// </remarks>
        protected override string Quote(string name)
        {
            return OpenQuote + name.Replace(CloseQuote.ToString(), new string(CloseQuote, 2)) + CloseQuote;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="quoted"></param>
        /// <returns></returns>
        public override string UnQuote(string quoted)
        {
            if (IsQuoted(quoted))
            {
                quoted = quoted.Substring(1, quoted.Length - 2);
            }

            return quoted.Replace(new string(CloseQuote, 2), CloseQuote.ToString());
        }

        public override JoinFragment CreateOuterJoinFragment()
        {
            return new JetJoinFragment();
        }

        /// <summary>
        /// Create an <c>CaseFragment</c> for this dialect
        /// </summary>
        /// <returns></returns>
        public override CaseFragment CreateCaseFragment()
        {
            return new JetCaseFragment(this);
        }

        /// <summary>
        /// The SQL literal value to which this database maps boolean values. 
        /// </summary>
        /// <param name="value">The boolean value </param>
        /// <returns>
        /// The appropriate SQL literal. 
        /// </returns>
        public override string ToBooleanValueString(bool value)
        {
            if (value)
            {
                return "true";
            }
            else
            {
                return "false";
            }
        }

    }
}
