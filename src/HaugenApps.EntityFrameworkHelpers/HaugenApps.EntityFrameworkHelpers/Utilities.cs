using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using HaugenApps.ChangeTracking;

namespace HaugenApps.EntityFrameworkHelpers
{
    class Utilities
    {
        public static IEnumerable<IGrouping<IEnumerable<PropertyInfo>, PropertyWatcher<T>>> GroupPropertyWatchers<T>(IEnumerable<PropertyWatcher<T>> Watchers)
        {
            return Watchers.GroupBy(c => c.GetValues().Select(x => x.Key), c => c, new HeapComparer<PropertyInfo>());
        }

        private class HeapComparer<T> : IEqualityComparer<IEnumerable<T>>
        {
            public HeapComparer(IEqualityComparer<T> comparer)
            {
                this.Comparer = comparer;
            }

            public IEqualityComparer<T> Comparer { get; private set; }

            public HeapComparer()
                : this(EqualityComparer<T>.Default)
            {
            }

            public bool Equals(IEnumerable<T> x, IEnumerable<T> y)
            {
                var xList = x as IList<T> ?? x.ToList();
                var yList = y as IList<T> ?? y.ToList();

                return xList.GroupJoin(yList, a => a, b => b, (a, b) => b.Count() == 1, this.Comparer)
                            .Concat(yList.GroupJoin(xList, a => a, b => b, (a, b) => b.Count() == 1, this.Comparer))
                            .All(c => c);
            }

            public int GetHashCode(IEnumerable<T> obj)
            {
                return obj.Aggregate(0, (current, v) => current | this.Comparer.GetHashCode(v));
            }
        }

        public static IEnumerable<PropertyInfo> GetProperties<T>(Expression<Func<T, object>> Expr)
        {
            if (Expr.Body is NewExpression)
            {
                return ((NewExpression)Expr.Body).Arguments.Cast<MemberExpression>().Select(c => c.Member).Cast<PropertyInfo>();
            }
            else if (Expr.Body is MemberExpression)
            {
                return new[] { (PropertyInfo)((MemberExpression)Expr.Body).Member };
            }

            return Enumerable.Empty<PropertyInfo>();
        }
    }
}
