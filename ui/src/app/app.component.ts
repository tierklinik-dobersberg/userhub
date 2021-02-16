import { Component, isDevMode, OnInit } from '@angular/core';
import { NavigationEnd, NavigationStart, Router } from '@angular/router';
import { NzMessageService } from 'ng-zorro-antd/message';
import { forkJoin, of } from 'rxjs';
import { catchError, filter, map, mergeMap, share } from 'rxjs/operators';
import { ConfigAPI, IdentityAPI, Permission, Profile, ProfileWithAvatar, UIConfig, VoiceMailAPI, VoiceMailRecording } from './api';
import { LayoutService } from './layout.service';

interface MenuEntry {
  Icon: string;
  Link: string;
  Text: string;
  BlankTarget: boolean;
}

interface SubMenu {
  Text: string;
  Items: MenuEntry[];
}

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
  isCollapsed = false;
  isDevMode = isDevMode();

  profile: ProfileWithAvatar | null = null;
  rootLinks: MenuEntry[] = []
  subMenus: SubMenu[] = [];
  menuMode: 'inline' | 'vertical' = 'inline';
  mailboxes: string[] = [];

  isLogin = this.router.events
    .pipe(
      filter(e => e instanceof NavigationEnd),
      map(() => {
        const isLogin = this.router.url.startsWith("/login")
        return isLogin;
      }),
      share()
    );

  /** Returns true if the user has (at least read-only) access to the roster */
  get hasRoster() {
    return this.identity.hasPermission(Permission.RosterRead);
  }

  /** Returns true if the user can see voicemail records */
  get hasVoiceMail() {
    return this.identity.hasPermission(Permission.VoicemailRead)
  }

  /** Returns true if the user can see calllogs */
  get hasCallLog() {
    return this.identity.hasPermission(Permission.CalllogReadRecords)
  }

  /** Returns true if the user can see customer records */
  get hasCustomers() {
    return this.identity.hasPermission(Permission.CustomerRead)
  }

  constructor(
    private identity: IdentityAPI,
    private configapi: ConfigAPI,
    private router: Router,
    private nzMessage: NzMessageService,
    public layout: LayoutService,
    private voice: VoiceMailAPI,
  ) {
  }

  logout() {
    this.identity.logout()
      .subscribe(() => this.router.navigate(['/', 'login']))
  }

  toggleMenu() {
    this.isCollapsed = !this.isCollapsed;
  }

  ngOnInit() {

    this.layout.change.subscribe(() => {
      this.isCollapsed = !this.layout.isTabletLandscapeUp;
    });

    this.configapi.change
      .subscribe(cfg => this.applyConfig(cfg));

    this.identity.profileChange
      .subscribe({
        next: result => {
          this.profile = result;
        },
        error: console.error,
      });

    this.isCollapsed = this.layout.isPhone;

    this.router.events
      .subscribe(event => {
        if (event instanceof NavigationEnd && this.layout.isPhone) {
          this.isCollapsed = true;
        }
      })
  }

  private applyConfig(cfg: UIConfig | null) {
    let menus = new Map<string, SubMenu>();
    this.rootLinks = [];

    (cfg?.ExternalLinks || []).forEach(link => {
      if (!link.ParentMenu) {
        this.rootLinks.push(link)
        return
      }

      let m = menus.get(link.ParentMenu);
      if (!m) {
        m = {
          Text: link.ParentMenu,
          Items: []
        }
        menus.set(link.ParentMenu, m)
      }

      m.Items.push(link)
    })

    this.subMenus = Array.from(menus.values());
    this.voice.listMailboxes().subscribe(mailboxes => {
      console.log(mailboxes);
      this.mailboxes = mailboxes;
    })
  }
}
