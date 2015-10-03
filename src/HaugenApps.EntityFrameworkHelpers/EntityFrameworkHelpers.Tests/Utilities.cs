using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using HaugenApps.ChangeTracking;
using HaugenApps.EntityFrameworkHelpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EntityFrameworkHelpers.Tests
{
    //public class Foo
    //{
    //    public string PropertyA { get; set; }
    //    public string PropertyB { get; set; }
    //    public string PropertyC { get; set; }
    //}

    //[TestClass]
    //public class Utilities
    //{
    //    [TestMethod]
    //    public void Group()
    //    {
    //        var watchers = new[]
    //        {
    //            new PropertyWatcher<Foo>().Set(c => c.PropertyA, "A"),
    //            new PropertyWatcher<Foo>().Set(c => c.PropertyA, "A"),
    //            new PropertyWatcher<Foo>().Set(c => c.PropertyA, "A").Set(c => c.PropertyB, "B").Set(c => c.PropertyC, "C"),
    //            new PropertyWatcher<Foo>().Set(c => c.PropertyA, "A").Set(c => c.PropertyC, "C").Set(c => c.PropertyB, "B"),
    //            new PropertyWatcher<Foo>().Set(c => c.PropertyA, "A").Set(c => c.PropertyB, "B")
    //        };

    //        var ret = HaugenApps.EntityFrameworkHelpers.Utilities.GroupPropertyWatchers(watchers).ToList();

    //        Assert.AreEqual(3, ret.Count, "ret.Count != 3");
    //    }
    //}
}
