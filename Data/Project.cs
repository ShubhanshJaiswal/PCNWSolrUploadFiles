using System;
using System.Collections.Generic;

namespace PCNWSolrUploadFiles.Data;

public partial class Project
{
    public int ProjId { get; set; }

    public string? Title { get; set; }

    public string? LocAddr1 { get; set; }

    public string? LocAddr2 { get; set; }

    public string? LocCity { get; set; }

    public string? LocState { get; set; }

    public string? LocZip { get; set; }

    public DateTime? PreBidDt { get; set; }

    public DateTime? BidDt { get; set; }

    public string? CompleteDt { get; set; }

    public string? LastBidDt { get; set; }

    public string? IssuingOffice { get; set; }

    public decimal? RefundAmt { get; set; }

    public int? ProjTypeId { get; set; }

    public int? PlanNo { get; set; }

    public string? BidBond { get; set; }

    public string? EstCost { get; set; }

    public bool? PrevailingWage { get; set; }

    public string? SubApprov { get; set; }

    public bool? Mandatory { get; set; }

    public DateTime? ExtendedDt { get; set; }

    public int? BidNo { get; set; }

    public DateTime? ArrivalDt { get; set; }

    public DateTime? ResultDt { get; set; }

    public string? PreBidLoc { get; set; }

    public string? StrBidDt { get; set; }

    public string? StrPreBidDt { get; set; }

    public string? ProjNote { get; set; }

    public decimal? NonRefundAmt { get; set; }

    public string? Dfnote { get; set; }

    public DateTime? DupArDt { get; set; }

    public int? Dup1 { get; set; }

    public int? Dup2 { get; set; }

    public bool? FutureWork { get; set; }

    public string? Brnote { get; set; }

    public string? Story { get; set; }

    public float? AdSpacer { get; set; }

    public int? EstCostNum { get; set; }

    public bool? Publish { get; set; }

    public string? StoryUnf { get; set; }

    public DateTime? ImportDt { get; set; }

    public string? DiPath { get; set; }

    public bool? ShowOnWeb { get; set; }

    public string? DupTitle { get; set; }

    public string? PublishedFrom { get; set; }

    public DateTime? PublishedFromDt { get; set; }

    public bool? UnderCounter { get; set; }

    public bool? Recycle { get; set; }

    public bool? Hold { get; set; }

    public bool? CallBack { get; set; }

    public decimal? Deposit { get; set; }

    public decimal? ShipCheck { get; set; }

    public DateTime? CheckSentDt { get; set; }

    public int? SpecVols { get; set; }

    public int? DrawingVols { get; set; }

    public string? InternalNote { get; set; }

    public bool? ShowBr { get; set; }

    public string? EstCost2 { get; set; }

    public string? EstCost3 { get; set; }

    public string? EstCost4 { get; set; }

    public int? EstCostNum2 { get; set; }

    public int? EstCostNum3 { get; set; }

    public int? EstCostNum4 { get; set; }

    public bool? Uc { get; set; }

    public string? Ucpwd { get; set; }

    public string? SpecPath { get; set; }

    public string? DrawingPath { get; set; }

    public string? MaxViewPath { get; set; }

    /// <summary>
    /// used in Maxviewprep to mark G3 prepped
    /// </summary>
    public bool? TopChk { get; set; }

    public bool? DwChk { get; set; }

    public bool? SpcChk { get; set; }

    public bool? RenChk { get; set; }

    public string? OnlineNote { get; set; }

    public int? RegionId { get; set; }

    public DateTime? PreBidDt2 { get; set; }

    public string? StrPreBidDt2 { get; set; }

    public string? PreBidLoc2 { get; set; }

    public bool? Mandatory2 { get; set; }

    public bool? PrebidAnd { get; set; }

    public bool? PrebidOr { get; set; }

    public byte[]? ProjTimeStamp { get; set; }

    public string? Contact { get; set; }

    public string? Ucpwd2 { get; set; }

    public bool? S11x17 { get; set; }

    public bool? S18x24 { get; set; }

    public bool? S24x36 { get; set; }

    public bool? S30x42 { get; set; }

    public bool? NoSpecs { get; set; }

    public bool? SpecsOnPlans { get; set; }

    public bool? InternetDownload { get; set; }

    public string? BrresultsFrom { get; set; }

    public string? LocCity2 { get; set; }

    public string? LocState2 { get; set; }

    public string? LocCity3 { get; set; }

    public string? LocState3 { get; set; }

    public bool? DirtId { get; set; }

    public bool? S36x48 { get; set; }

    public DateTime? BidDt2 { get; set; }

    public string? StrBidDt2 { get; set; }

    public DateTime? BidDt3 { get; set; }

    public string? StrBidDt3 { get; set; }

    public DateTime? BidDt4 { get; set; }

    public string? StrBidDt4 { get; set; }

    public int? PlanNoMain { get; set; }

    public int? ProjIdMain { get; set; }

    public bool? ShowToAll { get; set; }

    public string? Phlnote { get; set; }

    public bool? Phldone { get; set; }

    public string? PhlwebLink { get; set; }

    public DateTime? Phltimestamp { get; set; }

    public bool? BendPc { get; set; }

    public bool? NoPrint { get; set; }

    public bool? Ucpublic { get; set; }

    public bool? BidPkg { get; set; }

    public bool? BuildSolrIndex { get; set; }

    public double? Longitude { get; set; }

    public double? Latitude { get; set; }

    public string? StrAddenda { get; set; }

    public DateTime? SolrIndexDt { get; set; }

    public bool? IndexPdffiles { get; set; }

    public DateTime? SolrIndexPdfdt { get; set; }

    public int? ProjSubTypeId { get; set; }

    public string? ProjNumber { get; set; }

    public string? ProjScope { get; set; }

    public DateTime? BidDt5 { get; set; }

    public string? StrBidDt5 { get; set; }

    public DateTime? PreBidDt3 { get; set; }

    public string? StrPreBidDt3 { get; set; }

    public DateTime? PreBidDt4 { get; set; }

    public string? StrPreBidDt4 { get; set; }

    public DateTime? PreBidDt5 { get; set; }

    public string? StrPreBidDt5 { get; set; }

    public string? PreBidLoc3 { get; set; }

    public string? PreBidLoc4 { get; set; }

    public string? PreBidLoc5 { get; set; }

    public string? EstCost5 { get; set; }

    public int? EstCostNum5 { get; set; }

    public int? MemberId { get; set; }

    public string? CreatedBy { get; set; }

    public DateTime? CreatedDate { get; set; }

    public string? UpdatedBy { get; set; }

    public DateTime? UpdatedDate { get; set; }

    public string? MachineIp { get; set; }

    public bool? IsActive { get; set; }

    public int? CountyId { get; set; }

    public string? BackProjNumber { get; set; }

    public string? Undecided { get; set; }

    public string? AddendaNote { get; set; }

    public string? PrebidNote { get; set; }
}
