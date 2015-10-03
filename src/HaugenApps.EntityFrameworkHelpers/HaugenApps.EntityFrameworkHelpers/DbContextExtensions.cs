using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.EntityClient;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Spatial;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using HaugenApps.ChangeTracking;
using HaugenApps.HaugenCore;

namespace HaugenApps.EntityFrameworkHelpers
{
    public static class DbContextExtensions
    {
        public static async Task DeleteAsync<T>(this DbContext context, IQueryable<T> query)
                    where T : class
        {
            context.Set<T>().RemoveRange(await query.ToArrayAsync());
        }

        public static async Task UpdateAsync<T>(this DbContext context, IQueryable<T> query, PropertyWatcher<T> Saved)
            where T : class, new()
        {
            //var itemParam = Expression.Parameter(typeof(T), "x");

            //var primaryKeys = Utilities.GetPrimaryKeys(typeof(T), context);
            //var primaryKeyArgs = primaryKeys.Select(c => Expression.PropertyOrField(itemParam, c.Name)).ToArray();

            //var selector = Expression.Call(typeof(Tuple), "Create", primaryKeyArgs.Select(c => c.Type).ToArray(), primaryKeyArgs);
            //var lambda = Expression.Lambda<Func<T, object>>(selector, itemParam);

            //foreach (var item in query.Select(lambda.Compile()))
            //{
            //    T ret = new T();

            //    foreach (var v in primaryKeys.Select((c, index) => new { c, index }))
            //    {
            //        var tupleProp = item.GetType().GetProperty("Item" + v.index).GetValue(item);

            //        v.c.SetValue(ret, tupleProp);
            //    }

            //    var updated = context.Entry(ret);

            //    Saved.LoadToInstance(ref ret);

            //    foreach (var v in Saved.GetValues())
            //    {
            //        updated.Property(v.Key.Name).IsModified = true;
            //    }
            //}

            await query.ForEachAsync(item =>
            {
                var ret = item;

                var updated = context.Entry(ret);

                Saved.LoadToInstance(ref ret);

                foreach (var v in Saved.GetValues())
                {
                    updated.Property(v.Key.Name).IsModified = true;
                }
            });

            context.Configuration.ValidateOnSaveEnabled = false;
        }

        public static async Task UpdateManyToManyAsync<TLeftKey, TRightKey>(this DbContext context, string Table, string LeftKeyName, string RightKeyName, TLeftKey LeftKeyValue, IEnumerable<TRightKey> Values)
        {
            List<string> toInsert = new List<string>();
            List<string> parameterNames = new List<string>();
            List<object> parameters = new List<object>();

            parameters.Add(LeftKeyValue);

            bool any = false;
            foreach (var v in Values)
            {
                any = true;

                string paramName = string.Format("@p{0}", parameters.Count);

                parameterNames.Add(paramName);

                toInsert.Add(string.Format("(@p0, {0})", paramName));
                parameters.Add(GetParameter(v));
            }

            if (any)
            {
                string query = string.Format(@"MERGE {0} T
                                           USING (VALUES {3}) AS S ([{1}], [{2}])
                                           ON T.[{1}] = S.[{1}] AND T.[{2}] = S.[{2}]
                                           WHEN NOT MATCHED THEN
                                                 INSERT ([{1}], [{2}]) VALUES (S.{1}, S.{2});", Table, LeftKeyName, RightKeyName, string.Join(", ", toInsert));

                await context.Database.ExecuteSqlCommandAsync(query, parameters.ToArray());

                query = string.Format("DELETE FROM {0} WHERE [{0}].[{1}] = @p0 AND [{0}].[{2}] NOT IN ({3})", Table, LeftKeyName, RightKeyName, string.Join(", ", parameterNames));

                await context.Database.ExecuteSqlCommandAsync(query, parameters.ToArray());
            }
            else
            {
                string query = string.Format("DELETE FROM {0} WHERE [{0}].[{1}] = @p0", Table, LeftKeyName);

                await context.Database.ExecuteSqlCommandAsync(query, parameters.ToArray());
            }
        }

        public static void Update<T, T2>(this DbContext context, PropertyWatcher<T> Saved, Expression<Func<T, T2>> KeyField, T2 KeyValue)
            where T : class, new()
        {
            var ret = new T();
            Saved.LoadToInstance(ref ret);

            var prop = Reflection.GetPropertyInfo(KeyField);
            prop.SetValue(ret, KeyValue);

            context.Set<T>().Attach(ret);

            var updated = context.Entry<T>(ret);

            foreach (var v in Saved.GetValues())
            {
                updated.Property(v.Key.Name).IsModified = true;
            }

            context.Configuration.ValidateOnSaveEnabled = false;
        }
        public static Task<T2> UpdateAsync<T, T2>(this DbContext context, PropertyWatcher<T> Saved, Expression<Func<T, T2>> KeyField, T2 KeyValue, Expression<Func<T, T2>> OutputColumn, bool OutputInserted)
        {
            string table = GetTableName(typeof(T), context);

            string query = string.Format("UPDATE {0} SET {1} OUTPUT {3}.{4} WHERE {2} = @p0", table, string.Join(", ", Saved.GetValues().Select((c, index) => string.Format("{0} = @p{1}", GetColumnName(c.Key), index + 1))), GetColumnName(Reflection.GetPropertyInfo(KeyField)), OutputInserted ? "INSERTED" : "DELETED", GetColumnName(Reflection.GetPropertyInfo(OutputColumn)));

            return context.Database.SqlQuery<T2>(query, Saved.GetValues().Select(c => GetParameter(c.Value)).ToArray()).FirstAsync();
        }

        public static Task<int> InsertAsync<T>(this DbContext context, PropertyWatcher<T> Saved)
        {
            string table = GetTableName(typeof(T), context);

            var vals = Saved.GetValues().ToArray();

            if (vals.Length == 0)
            {
                string query = string.Format("INSERT INTO {0} DEFAULT VALUES", table);

                return context.Database.ExecuteSqlCommandAsync(query, vals.Select(c => GetParameter(c.Value)).ToArray());
            }
            else
            {
                string query = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", table,
                    string.Join(", ", vals.Select(c => GetColumnName(c.Key))),
                    string.Join(", ", Enumerable.Range(0, vals.Length).Select(c => "@p" + c)));

                return context.Database.ExecuteSqlCommandAsync(query, vals.Select((c, i) => GetParameter(c.Value)).ToArray());
            }
        }

        public static Task<T2> InsertAsync<T, T2>(this DbContext context, PropertyWatcher<T> Saved, Expression<Func<T, T2>> OutputColumn)
        {
            string table = GetTableName(typeof(T), context);

            var vals = Saved.GetValues().ToArray();
            var outputColumnName = GetColumnName(Reflection.GetPropertyInfo(OutputColumn));

            if (vals.Length == 0)
            {
                string query = string.Format("INSERT INTO {0} OUTPUT INSERTED.{1} DEFAULT VALUES", table, outputColumnName);

                return context.Database.SqlQuery<T2>(query).FirstAsync();
            }
            else
            {
                string query = string.Format("INSERT INTO {0} ({1}) OUTPUT INSERTED.{2} VALUES ({3})", table,
                    string.Join(", ", vals.Select(c => GetColumnName(c.Key))),
                    outputColumnName,
                    string.Join(", ", Enumerable.Range(0, vals.Length).Select(c => "@p" + c)));

                return context.Database.SqlQuery<T2>(query, vals.Select((c, i) => GetParameter(c.Value)).ToArray()).FirstAsync();
            }
        }

        public static Task<T> InsertAndFetchAsync<T>(this DbContext context, PropertyWatcher<T> Saved)
        {
            string table = GetTableName(typeof(T), context);

            var vals = Saved.GetValues().ToArray();

            if (vals.Length == 0)
            {
                string query = string.Format("INSERT INTO {0} OUTPUT INSERTED.* DEFAULT VALUES", table);

                return context.Database.SqlQuery<T>(query).FirstAsync();
            }
            else
            {
                string query = string.Format("INSERT INTO {0} ({1}) OUTPUT INSERTED.* VALUES ({2})", table,
                    string.Join(", ", vals.Select(c => GetColumnName(c.Key))),
                    string.Join(", ", Enumerable.Range(0, vals.Length).Select(c => "@p" + c)));

                return context.Database.SqlQuery<T>(query, vals.Select(c => GetParameter(c.Value)).ToArray()).FirstAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this DbContext context, PropertyWatcher<T> Saved)
            where T : class, new()
        {
            string table = GetTableName(typeof(T), context);

            string query = string.Format("DELETE FROM {0} WHERE {1}", table, string.Join(" AND ", Saved.GetValues().Select((c, index) => string.Format("{0} = @p{1}", GetColumnName(c.Key), index))));

            return context.Database.ExecuteSqlCommandAsync(query, Saved.GetValues().Select(c => GetParameter(c.Value)).ToArray());
        }

        public static Task<int> MergeAsync<T>(this DbContext context, Expression<Func<T, object>> On, params PropertyWatcher<T>[] Entities)
        {
            return MergeAsync(context, On, Entities.AsEnumerable());
        }

        public static Task<int> MergeAsync<T>(this DbContext context, Expression<Func<T, object>> On, IEnumerable<PropertyWatcher<T>> Entities)
        {
            return MergeAsync(context, true, On, Entities);
        }

        public static async Task<int> MergeAsync<T>(this DbContext context, bool Update, Expression<Func<T, object>> On, IEnumerable<PropertyWatcher<T>> Entities)
        {
            var onProps = Utilities.GetProperties(On).ToList();

            var entityList = Entities as List<PropertyWatcher<T>> ?? Entities.ToList();

            var table = GetTableName(typeof (T), context);

            if (!entityList.SelectMany(c => onProps.Select(c.HasValue)).All(c => c))
                throw new ArgumentException("All \"On\" properties must be present in all property watchers.");

            var onClause = string.Join(" AND ", onProps.Select(c => string.Format("((T.{0} IS NULL AND S.{0} IS NULL) OR T.{0} = S.{0})", GetColumnName(c))));

            var ret = 0;

            using (var trans = context.Database.BeginTransaction())
            { 
                foreach (var v in Utilities.GroupPropertyWatchers(entityList))
                {
                    var parameters = new List<object>();

                    StringBuilder columnList = new StringBuilder(), sourceColumnList = new StringBuilder(), toUpdate = new StringBuilder(), toInsert = new StringBuilder();

                    bool first = true;
                    foreach (var prop in v.Key)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            columnList.Append(", ");
                            sourceColumnList.Append(", ");
                            toUpdate.Append(", ");
                        }

                        var columnName = GetColumnName(prop);

                        columnList.Append(columnName);
                        sourceColumnList.AppendFormat("S.{0}", columnName);
                        toUpdate.AppendFormat("{0} = S.{0}", columnName);
                    }

                    first = true;
                    foreach (var watcher in v)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            toInsert.Append(", ");
                        }

                        toInsert.Append("(");

                        bool firstProp = true;
                        foreach (var prop in v.Key)
                        {
                            if (firstProp)
                            {
                                firstProp = false;
                            }
                            else
                            {
                                toInsert.Append(", ");
                            }

                            toInsert.AppendFormat("@p{0}", parameters.Count);
                            parameters.Add(GetParameter(watcher.Get(prop)));
                        }
                        toInsert.Append(")");
                    }

                    string query;

                    if (Update)
                    {
                        query = string.Format(@"MERGE {0} T
                                                USING (VALUES {4}) AS S ({2})
                                                ON {1}
                                                WHEN MATCHED THEN
                                                    UPDATE SET {5}
                                                WHEN NOT MATCHED THEN
                                                    INSERT ({2}) VALUES ({3});", table, onClause, columnList, sourceColumnList, toInsert, toUpdate);
                    }
                    else
                    {
                        query = string.Format(@"INSERT INTO {0} ({2})
                                                SELECT {2}
                                                FROM (VALUES {3}) AS S ({2})
                                                WHERE NOT EXISTS (SELECT * FROM {0} T WHERE {1})", table, onClause, columnList, toInsert);
                    }

                    ret += await context.Database.ExecuteSqlCommandAsync(query, parameters.ToArray());
                }

                trans.Commit();
            }

            return ret;
        }

        private static object GetParameter(object data)
        {
            if (data is DbGeography)
            {
                return ((DbGeography) data).WellKnownValue.WellKnownText;
            }

            return data;
        }

        private static string GetColumnName(PropertyInfo property)
        {
            var tableAttr = property.GetCustomAttributes<ColumnAttribute>().FirstOrDefault();

            return string.Format("[{0}]", tableAttr == null ? property.Name : tableAttr.Name);
        }

        internal static string GetTableName(Type type, DbContext context)
        {
            var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;

            // Get the part of the model that contains info about the actual CLR types
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            // Get the entity type from the model that maps to the CLR type
            var entityType = metadata
                    .GetItems<EntityType>(DataSpace.OSpace)
                    .Single(e => objectItemCollection.GetClrType(e) == type);

            // Get the entity set that uses this entity type
            var entitySet = metadata
                .GetItems<EntityContainer>(DataSpace.CSpace)
                .Single()
                .EntitySets
                .Single(s => s.ElementType.Name == entityType.Name);

            // Find the mapping between conceptual and storage model for this entity set
            var mapping = metadata.GetItems<EntityContainerMapping>(DataSpace.CSSpace)
                    .Single()
                    .EntitySetMappings
                    .Single(s => s.EntitySet == entitySet);

            // Find the storage entity set (table) that the entity is mapped
            var table = mapping
                .EntityTypeMappings.First()
                .Fragments.First()
                .StoreEntitySet;

            string schema = (string)table.MetadataProperties["Schema"].Value ?? table.Schema;

            // Return the table name from the storage entity set
            return (string.IsNullOrWhiteSpace(schema) ? "" : "[" + schema + "].") + "[" + ((string)table.MetadataProperties["Table"].Value ?? table.Name) + "]";
        }
        internal static IEnumerable<PropertyInfo> GetPrimaryKeys(Type type, DbContext context)
        {
            var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;

            // Get the part of the model that contains info about the actual CLR types
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            // Get the entity type from the model that maps to the CLR type
            var entityType = metadata
                    .GetItems<EntityType>(DataSpace.OSpace)
                    .Single(e => objectItemCollection.GetClrType(e) == type);

            return entityType.KeyProperties.Select(k => type.GetProperty(k.Name)).ToArray();
        }

        //private static IEnumerable<EdmProperty> GetProperties(DbContext context, Type type, IEnumerable<PropertyInfo> Properties)
        //{
        //    var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;

        //    // Get the part of the model that contains info about the actual CLR types
        //    var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

        //    // Get the entity type from the model that maps to the CLR type
        //    var entityType = metadata
        //            .GetItems<EntityType>(DataSpace.OSpace)
        //            .Single(e => objectItemCollection.GetClrType(e) == type);

        //    return entityType.Properties.Join(Properties, Property => Property.);
        //} 
    }
}