using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace PCNWSolrUploadFiles.Data;

public partial class PcnwprojectDbContext : DbContext
{
    public PcnwprojectDbContext()
    {
    }

    public PcnwprojectDbContext(DbContextOptions<PcnwprojectDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Project> Projects { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Project>(entity =>
        {
            entity.HasKey(e => e.ProjId).HasName("PK_tblProject");

            entity.ToTable("Project", tb => tb.HasTrigger("tr_Project_U"));

            entity.HasIndex(e => e.LocState, "LocState");

            entity.HasIndex(e => e.ProjNumber, "NOC_INDX_tblProject_ProjNumber");

            entity.HasIndex(e => e.BidDt, "PK_tblProjectBidDt");

            entity.HasIndex(e => e.PlanNo, "PK_tblProjectPlanNo");

            entity.HasIndex(e => e.Title, "PK_tblProjectTitle");

            entity.HasIndex(e => new { e.BidDt3, e.BidDt, e.BidDt2, e.BidDt4, e.PlanNo }, "_dta_index_tblProject_7_772197801__K101_K9_K99_K103_K15_1_2_5_17_26_39_43_49_64");

            entity.HasIndex(e => e.BidDt4, "_dta_index_tblProject_7_772197801__K103");

            entity.HasIndex(e => e.BidDt2, "_dta_index_tblProject_7_772197801__K99");

            entity.HasIndex(e => new { e.BidDt, e.ProjId, e.PlanNo }, "_dta_index_tblProject_7_772197801__K9_K1_K15_2_26_68_84_85_86_87");

            entity.Property(e => e.AddendaNote).HasMaxLength(2000);
            entity.Property(e => e.ArrivalDt).HasColumnType("smalldatetime");
            entity.Property(e => e.BackProjNumber)
                .HasMaxLength(50)
                .HasDefaultValueSql("((0))");
            entity.Property(e => e.BendPc)
                .HasDefaultValue(false)
                .HasColumnName("BendPC");
            entity.Property(e => e.BidBond).HasMaxLength(20);
            entity.Property(e => e.BidDt).HasColumnType("datetime");
            entity.Property(e => e.BidDt2).HasColumnType("datetime");
            entity.Property(e => e.BidDt3).HasColumnType("datetime");
            entity.Property(e => e.BidDt4).HasColumnType("datetime");
            entity.Property(e => e.BidDt5).HasColumnType("datetime");
            entity.Property(e => e.BidPkg).HasDefaultValue(false);
            entity.Property(e => e.Brnote)
                .HasMaxLength(180)
                .HasColumnName("BRNote");
            entity.Property(e => e.BrresultsFrom)
                .HasMaxLength(180)
                .HasColumnName("BRResultsFrom");
            entity.Property(e => e.BuildSolrIndex).HasDefaultValue(true);
            entity.Property(e => e.CallBack).HasDefaultValue(false);
            entity.Property(e => e.CheckSentDt).HasColumnType("smalldatetime");
            entity.Property(e => e.CompleteDt).HasMaxLength(150);
            entity.Property(e => e.Contact)
                .HasMaxLength(50)
                .IsUnicode(false)
                .IsFixedLength();
            entity.Property(e => e.CountyId)
                .HasDefaultValue(0)
                .HasColumnName("CountyID");
            entity.Property(e => e.CreatedBy)
                .HasMaxLength(150)
                .HasColumnName("createdBy");
            entity.Property(e => e.CreatedDate)
                .HasColumnType("datetime")
                .HasColumnName("createdDate");
            entity.Property(e => e.Deposit).HasColumnType("money");
            entity.Property(e => e.Dfnote)
                .HasColumnType("ntext")
                .HasColumnName("DFNote");
            entity.Property(e => e.DiPath).HasMaxLength(100);
            entity.Property(e => e.DirtId)
                .HasDefaultValue(false)
                .HasColumnName("DirtID");
            entity.Property(e => e.DrawingPath).HasMaxLength(100);
            entity.Property(e => e.DupArDt).HasColumnType("smalldatetime");
            entity.Property(e => e.DupTitle).HasMaxLength(20);
            entity.Property(e => e.DwChk).HasDefaultValue(false);
            entity.Property(e => e.EstCost).HasMaxLength(70);
            entity.Property(e => e.EstCost2).HasMaxLength(70);
            entity.Property(e => e.EstCost3).HasMaxLength(70);
            entity.Property(e => e.EstCost4).HasMaxLength(70);
            entity.Property(e => e.EstCost5).HasMaxLength(150);
            entity.Property(e => e.ExtendedDt).HasColumnType("datetime");
            entity.Property(e => e.FutureWork).HasDefaultValue(false);
            entity.Property(e => e.Hold).HasDefaultValue(false);
            entity.Property(e => e.ImportDt).HasColumnType("smalldatetime");
            entity.Property(e => e.IndexPdffiles)
                .HasDefaultValue(true)
                .HasColumnName("IndexPDFFiles");
            entity.Property(e => e.InternalNote).HasColumnType("ntext");
            entity.Property(e => e.InternetDownload).HasDefaultValue(false);
            entity.Property(e => e.IsActive).HasDefaultValue(true);
            entity.Property(e => e.IssuingOffice).HasMaxLength(80);
            entity.Property(e => e.LastBidDt).HasMaxLength(150);
            entity.Property(e => e.Latitude).HasDefaultValue(0.0);
            entity.Property(e => e.LocAddr1).HasMaxLength(150);
            entity.Property(e => e.LocAddr2).HasMaxLength(150);
            entity.Property(e => e.LocCity).HasMaxLength(50);
            entity.Property(e => e.LocCity2).HasMaxLength(50);
            entity.Property(e => e.LocCity3).HasMaxLength(50);
            entity.Property(e => e.LocState).HasMaxLength(2);
            entity.Property(e => e.LocState2).HasMaxLength(2);
            entity.Property(e => e.LocState3).HasMaxLength(2);
            entity.Property(e => e.LocZip).HasMaxLength(10);
            entity.Property(e => e.Longitude).HasDefaultValue(0.0);
            entity.Property(e => e.MachineIp)
                .HasMaxLength(250)
                .HasColumnName("machineIP");
            entity.Property(e => e.Mandatory).HasDefaultValue(false);
            entity.Property(e => e.Mandatory2).HasDefaultValue(false);
            entity.Property(e => e.MaxViewPath).HasMaxLength(200);
            entity.Property(e => e.MemberId).HasColumnName("memberId");
            entity.Property(e => e.NoPrint).HasDefaultValue(false);
            entity.Property(e => e.NoSpecs).HasDefaultValue(false);
            entity.Property(e => e.NonRefundAmt).HasColumnType("money");
            entity.Property(e => e.OnlineNote).HasMaxLength(80);
            entity.Property(e => e.Phldone)
                .HasDefaultValue(false)
                .HasColumnName("PHLdone");
            entity.Property(e => e.Phlnote)
                .HasMaxLength(150)
                .HasColumnName("PHLnote");
            entity.Property(e => e.Phltimestamp)
                .HasColumnType("datetime")
                .HasColumnName("PHLtimestamp");
            entity.Property(e => e.PhlwebLink)
                .HasMaxLength(150)
                .HasColumnName("PHLwebLink");
            entity.Property(e => e.PreBidDt).HasColumnType("datetime");
            entity.Property(e => e.PreBidDt2).HasColumnType("datetime");
            entity.Property(e => e.PreBidDt3).HasColumnType("datetime");
            entity.Property(e => e.PreBidDt4).HasColumnType("datetime");
            entity.Property(e => e.PreBidDt5).HasColumnType("datetime");
            entity.Property(e => e.PreBidLoc).HasMaxLength(150);
            entity.Property(e => e.PreBidLoc2).HasMaxLength(150);
            entity.Property(e => e.PreBidLoc3).HasMaxLength(150);
            entity.Property(e => e.PreBidLoc4).HasMaxLength(150);
            entity.Property(e => e.PreBidLoc5).HasMaxLength(150);
            entity.Property(e => e.PrebidAnd)
                .HasDefaultValue(false)
                .HasColumnName("PrebidAND");
            entity.Property(e => e.PrebidNote).HasMaxLength(2000);
            entity.Property(e => e.PrebidOr)
                .HasDefaultValue(false)
                .HasColumnName("PrebidOR");
            entity.Property(e => e.PrevailingWage).HasDefaultValue(false);
            entity.Property(e => e.ProjNote).HasColumnType("ntext");
            entity.Property(e => e.ProjNumber)
                .HasMaxLength(50)
                .IsUnicode(false);
            entity.Property(e => e.ProjScope).HasMaxLength(250);
            entity.Property(e => e.ProjTimeStamp)
                .IsRowVersion()
                .IsConcurrencyToken();
            entity.Property(e => e.Publish).HasDefaultValue(false);
            entity.Property(e => e.PublishedFrom).HasMaxLength(30);
            entity.Property(e => e.PublishedFromDt).HasColumnType("smalldatetime");
            entity.Property(e => e.Recycle).HasDefaultValue(false);
            entity.Property(e => e.RefundAmt).HasColumnType("money");
            entity.Property(e => e.RegionId).HasColumnName("RegionID");
            entity.Property(e => e.RenChk).HasDefaultValue(false);
            entity.Property(e => e.ResultDt).HasColumnType("smalldatetime");
            entity.Property(e => e.S11x17).HasDefaultValue(false);
            entity.Property(e => e.S18x24).HasDefaultValue(false);
            entity.Property(e => e.S24x36).HasDefaultValue(false);
            entity.Property(e => e.S30x42).HasDefaultValue(false);
            entity.Property(e => e.S36x48)
                .HasDefaultValue(false)
                .HasColumnName("S36X48");
            entity.Property(e => e.ShipCheck).HasColumnType("money");
            entity.Property(e => e.ShowBr)
                .HasDefaultValue(false)
                .HasColumnName("ShowBR");
            entity.Property(e => e.ShowOnWeb).HasDefaultValue(false);
            entity.Property(e => e.ShowToAll).HasDefaultValue(false);
            entity.Property(e => e.SolrIndexDt).HasColumnType("datetime");
            entity.Property(e => e.SolrIndexPdfdt)
                .HasColumnType("datetime")
                .HasColumnName("SolrIndexPDFDt");
            entity.Property(e => e.SpcChk).HasDefaultValue(false);
            entity.Property(e => e.SpecPath).HasMaxLength(100);
            entity.Property(e => e.SpecsOnPlans).HasDefaultValue(false);
            entity.Property(e => e.Story).HasColumnType("ntext");
            entity.Property(e => e.StoryUnf)
                .HasColumnType("ntext")
                .HasColumnName("StoryUNF");
            entity.Property(e => e.StrAddenda)
                .HasMaxLength(50)
                .HasDefaultValue("")
                .HasColumnName("strAddenda");
            entity.Property(e => e.StrBidDt)
                .HasMaxLength(30)
                .HasColumnName("strBidDt");
            entity.Property(e => e.StrBidDt2)
                .HasMaxLength(30)
                .HasColumnName("strBidDt2");
            entity.Property(e => e.StrBidDt3)
                .HasMaxLength(30)
                .HasColumnName("strBidDt3");
            entity.Property(e => e.StrBidDt4)
                .HasMaxLength(30)
                .HasColumnName("strBidDt4");
            entity.Property(e => e.StrBidDt5)
                .HasMaxLength(30)
                .HasColumnName("strBidDt5");
            entity.Property(e => e.StrPreBidDt)
                .HasMaxLength(30)
                .HasColumnName("strPreBidDt");
            entity.Property(e => e.StrPreBidDt2)
                .HasMaxLength(30)
                .HasColumnName("strPreBidDt2");
            entity.Property(e => e.StrPreBidDt3)
                .HasMaxLength(30)
                .HasColumnName("strPreBidDt3");
            entity.Property(e => e.StrPreBidDt4)
                .HasMaxLength(30)
                .HasColumnName("strPreBidDt4");
            entity.Property(e => e.StrPreBidDt5)
                .HasMaxLength(30)
                .HasColumnName("strPreBidDt5");
            entity.Property(e => e.SubApprov).HasMaxLength(50);
            entity.Property(e => e.Title).HasMaxLength(255);
            entity.Property(e => e.TopChk)
                .HasDefaultValue(false)
                .HasComment("used in Maxviewprep to mark G3 prepped");
            entity.Property(e => e.Uc)
                .HasDefaultValue(false)
                .HasColumnName("UC");
            entity.Property(e => e.Ucpublic)
                .HasDefaultValue(false)
                .HasColumnName("UCPublic");
            entity.Property(e => e.Ucpwd)
                .HasMaxLength(50)
                .HasColumnName("UCPWD");
            entity.Property(e => e.Ucpwd2)
                .HasMaxLength(50)
                .HasColumnName("UCPWD2");
            entity.Property(e => e.Undecided)
                .HasMaxLength(10)
                .IsUnicode(false);
            entity.Property(e => e.UnderCounter).HasDefaultValue(false);
            entity.Property(e => e.UpdatedBy)
                .HasMaxLength(150)
                .HasColumnName("updatedBy");
            entity.Property(e => e.UpdatedDate)
                .HasColumnType("datetime")
                .HasColumnName("updatedDate");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
