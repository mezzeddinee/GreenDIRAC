from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as _SiteDirector

class SiteDirector(_SiteDirector):
    """
    Custom Site Director that extends the standard DIRAC SiteDirector.
    """

    def initialize(self):
        """
        Called once at agent startup.
        We call the base initialize(), then add our own custom message.
        """
        # ✅ Call the parent (base) initialize() first
        result = super().initialize()

        # Check if base initialization succeeded
        if not result["OK"]:
            return result

        # 🟢 Add your own behavior here
        self.log.always("✅ GreenSiteDirector initialized successfully!")
        print("✅ GreenSiteDirector says hi — custom init complete!")

        # Return OK to signal successful init
        return result