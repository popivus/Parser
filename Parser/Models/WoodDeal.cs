using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parser.Models
{
    public class WoodDeal
    {
        public string sellerName { get; set; }
        public string sellerInn { get; set; }
        public string buyerName { get; set; }
        public string buyerInn { get; set; }
        public float woodVolumeBuyer { get; set; }
        public float woodVolumeSeller { get; set; }
        public string dealDate { get; set; }
        public string dealNumber { get; set; }
        public string __typename { get; set; }
    }
}
