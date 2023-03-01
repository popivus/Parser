using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parser.Models
{
    public class SearchReportWoodDeal
    {
        public WoodDeal[] content { get; set; }
        public int total { get; set; }
        public string __typename { get; set; }
    }
}
